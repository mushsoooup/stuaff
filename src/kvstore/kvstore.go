package kvstore

import (
	"bufio"
	"bytes"
	"cs/config"
	"cs/logger"
	"cs/pool"
	"cs/raft"
	"cs/schema"
	"encoding/gob"
	"encoding/json"
	"errors"
	"hash/fnv"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KVStore struct {
	cs          map[string]string
	course      map[string]string
	student     map[string]string
	p           pool.Pool
	readerPool  sync.Pool
	r           raft.Raft
	applyCh     chan raft.Packet
	mu          sync.Mutex
	notifyChans priorityQueue
}

const (
	CmdSearchCourse = iota
	CmdSearchStudent
	CmdSearchAll
	CmdChoose
	CmdDrop
)

const readTimeout = 1 * time.Second

type Command struct {
	Cmd   int
	Bulks []string
}

type result struct {
	success bool
	data    []byte
}

func (k *KVStore) initialize() {
	courseFile, _ := os.Open("./data/courses.txt")
	defer courseFile.Close()
	scanner := bufio.NewScanner(courseFile)
	courses := make([][]string, 0)
	for scanner.Scan() {
		course := strings.Split(scanner.Text(), " ")
		courses = append(courses, course)
	}
	for _, course := range courses {
		capacity, _ := strconv.Atoi(course[2])
		courseStr, _ := json.Marshal(schema.Course{CourseInfo: schema.CourseInfo{Id: course[0], Name: course[1]}, Capacity: capacity, Selected: 0})
		k.course[course[0]] = string(courseStr)
	}

	studentFile, _ := os.Open("./data/students.txt")
	defer studentFile.Close()
	scanner = bufio.NewScanner(studentFile)
	students := make([][]string, 0)
	for scanner.Scan() {
		student := strings.Split(scanner.Text(), " ")
		students = append(students, student)
	}
	for _, student := range students {
		studentStr, _ := json.Marshal(schema.Student{Id: student[0], Name: student[1]})
		k.student[student[0]] = string(studentStr)
	}
}

// Serve starts the KVStore
func (k *KVStore) Serve(path string) {
	gob.Register(Command{})
	k.cs = make(map[string]string)
	k.course = make(map[string]string)
	k.student = make(map[string]string)
	k.applyCh = make(chan raft.Packet)
	k.notifyChans.init()
	k.readerPool.New = func() any {
		return &bufio.Reader{}
	}
	endpoints := config.ReadConfig(path)
	ln, err := net.Listen("tcp", endpoints[0])
	if err != nil {
		logger.Debug("error listening addr %v", err)
		return
	}
	k.initialize()
	logger.Debug("length of map %d", len(k.student))
	addrs := k.generateAddrs(endpoints)
	go k.r.Make(addrs, endpoints, k.applyCh)
	go k.handleApply()
	k.p.Start(k.serveConn, 256*1024)
	for {
		c, err := ln.Accept()
		if err != nil {
			logger.Debug("error establishing connection %v", err)
			continue
		}
		if !k.p.Serve(c) {
			c.Close()
		}
	}
}

// handleApply handle messages from raft applyCh
func (k *KVStore) handleApply() {
	var failResult = result{success: false, data: nil}
	for {
		packet := <-k.applyCh
		res := result{}
		// Apply command
		cmd := packet.Command.(Command)
		switch cmd.Cmd {
		case CmdSearchCourse:
			courseStr, err := k.searchCourse(cmd.Bulks[0])
			res.success = true
			res.data = []byte(courseStr)
			if err != nil {
				res.success = false
			}
		case CmdSearchAll:
			res.data = k.searchAll()
			res.success = true
			if res.data == nil {
				res.success = false
			}
		case CmdSearchStudent:
			studentStr, err := k.searchStudent(cmd.Bulks[0])
			res.success = true
			res.data = []byte(studentStr)
			if err != nil {
				res.success = false
			}
		case CmdDrop:
			err := k.drop(cmd.Bulks[0], cmd.Bulks[1])
			res.success = true
			if err != nil {
				res.success = false
			}
		case CmdChoose:
			err := k.choose(cmd.Bulks[0], cmd.Bulks[1])
			res.success = true
			if err != nil {
				res.success = false
			}
		}
		// Search notify channels, send success to corresponding channel, fail preceding ones
		k.mu.Lock()
		for {
			item := k.notifyChans.front()
			if item == nil {
				break
			}
			// Front with lower term, outdated
			if item.term < packet.Term {
				item.notify <- failResult
				k.notifyChans.pop()
				continue
			} else if item.term > packet.Term {
				// Front with higher term, not a request from this server
				break
			}

			// Same for index
			if item.index < packet.Index {
				item.notify <- failResult
				k.notifyChans.pop()
				continue
			} else if item.index > packet.Index {
				break
			}
			// Right notify channel
			k.notifyChans.pop()
			item.notify <- res
			break
		}
		k.mu.Unlock()
	}
}

func (k *KVStore) searchCourse(courseId string) (string, error) {
	if k.course[courseId] == "" {
		return "", errors.New("invalid course_id")
	}
	return k.course[courseId], nil
}

func (k *KVStore) searchAll() []byte {
	result := make([]schema.Course, 0)
	for _, courseStr := range k.course {
		var course schema.Course
		json.Unmarshal([]byte(courseStr), &course)
		result = append(result, course)
	}
	resBytes, err := json.Marshal(result)
	if err != nil {
		return nil
	}
	return resBytes
}

func (k *KVStore) searchStudent(studentId string) (string, error) {
	studentStr, ok := k.student[studentId]
	if !ok {
		return "", errors.New("invalid stuid")
	}
	var student schema.Student
	json.Unmarshal([]byte(studentStr), &student)
	var result schema.StudentCourse
	result.Id = student.Id
	result.Name = student.Name
	courses := make([]schema.CourseInfo, 0)
	for key, sel := range k.cs {
		if strings.HasPrefix(key, studentId) {
			var s schema.Select
			json.Unmarshal([]byte(sel), &s)
			course, _ := k.searchCourse(s.CourseId)
			var cour schema.Course
			json.Unmarshal([]byte(course), &cour)
			courses = append(courses, schema.CourseInfo{
				Id:   cour.Id,
				Name: cour.Name,
			})
		}
	}
	result.Course = courses
	res, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

func (k *KVStore) drop(studentId, courseId string) error {
	course, err := k.searchCourse(courseId)
	if err != nil {
		return err
	}
	_, err = k.searchStudent(studentId)
	if err != nil {
		return err
	}
	if k.cs[studentId+courseId] == "" {
		return errors.New("invalid operate")
	}
	var res schema.Course
	json.Unmarshal([]byte(course), &res)
	res.Selected -= 1
	response, err := json.Marshal(res)
	if err != nil {
		return err
	}
	k.course[courseId] = string(response)
	delete(k.cs, studentId+courseId)
	return nil
}

func (k *KVStore) choose(studentId, courseId string) error {
	course, err := k.searchCourse(courseId)
	if err != nil {
		return err
	}
	_, err = k.searchStudent(studentId)
	if err != nil {
		return err
	}
	var res schema.Course
	json.Unmarshal([]byte(course), &res)
	if res.Selected == res.Capacity {
		return errors.New("full class")
	}
	res.Selected += 1
	response, err := json.Marshal(res)
	if err != nil {
		return err
	}
	k.course[courseId] = string(response)
	selectStr, _ := json.Marshal(schema.Select{
		StuID:    studentId,
		CourseId: courseId,
	})
	k.cs[studentId+courseId] = string(selectStr)
	return nil
}

func (k *KVStore) serveConn(c net.Conn) error {
	r := k.readerPool.Get().(*bufio.Reader)
	r.Reset(c)
	var err error
	for {
		c.SetReadDeadline(time.Now().Add(readTimeout))
		var cmd *Command
		cmd, err = parseData(r)
		if err != nil {
			break
		}
		res := k.handleCmd(cmd)
		_, err := c.Write(res)
		if err != nil {
			break
		}
	}
	k.readerPool.Put(r)
	return err
}

func (k *KVStore) handleCmd(cmd *Command) []byte {
	var res []byte
	switch cmd.Cmd {
	case CmdSearchCourse:
		res = k.submitSearchCourse(cmd)
	case CmdSearchAll:
		res = k.submitSearchAll(cmd)
	case CmdSearchStudent:
		res = k.submitSearchStudent(cmd)
	case CmdChoose:
		res = k.submitChoose(cmd)
	case CmdDrop:
		res = k.submitDrop(cmd)
	}
	return res
}

var (
	errorMessage = []byte("-ERROR\r\n\r\r")
	okMessage    = []byte("+OK\r\n\r\r")
)

func (k *KVStore) submitSearchCourse(cmd *Command) []byte {
	// Wrong command format
	if len(cmd.Bulks) != 1 {
		return errorMessage
	}
	// Lock here to avoid raft apply before notifyChans.push
	k.mu.Lock()
	// Send to raft
	leader, term, index := k.r.Submit(*cmd)
	// Return leader address
	if leader != "" {
		k.mu.Unlock()
		return []byte(leader + "\r\r")
	}
	// notify when command applied
	notify := make(chan result, 1)
	k.notifyChans.push(&item{term: term, index: index, notify: notify})
	k.mu.Unlock()
	res := <-notify
	if !res.success {
		return errorMessage
	}
	// Format res.data
	// `*1\r\n$3\r\n{}\r\n`
	res.data = append([]byte("*1\r\n$"+strconv.FormatInt(int64(len(res.data)), 10)+"\r\n"), res.data...)
	res.data = append(res.data, CRLF...)
	res.data = append(res.data, []byte("\r\r")...)
	return res.data
}

func (k *KVStore) submitSearchAll(cmd *Command) []byte {
	// Wrong command format
	if len(cmd.Bulks) != 0 {
		return errorMessage
	}
	// Lock here to avoid raft apply before notifyChans.push
	k.mu.Lock()
	// Send to raft
	leader, term, index := k.r.Submit(*cmd)
	// Return leader address
	if leader != "" {
		k.mu.Unlock()
		return []byte(leader + "\r\r")
	}
	// notify when command applied
	notify := make(chan result, 1)
	k.notifyChans.push(&item{term: term, index: index, notify: notify})
	k.mu.Unlock()
	res := <-notify
	if !res.success {
		return errorMessage
	}
	// Format res.data
	// `*1\r\n$3\r\n{}\r\n`
	res.data = append([]byte("*1\r\n$"+strconv.FormatInt(int64(len(res.data)), 10)+"\r\n"), res.data...)
	res.data = append(res.data, CRLF...)
	res.data = append(res.data, []byte("\r\r")...)
	return res.data
}

func (k *KVStore) submitChoose(cmd *Command) []byte {
	// Wrong command format
	if len(cmd.Bulks) != 2 {
		return errorMessage
	}
	// Lock here to avoid raft apply before notifyChans.push
	k.mu.Lock()
	// Send to raft
	leader, term, index := k.r.Submit(*cmd)
	// Return leader address
	if leader != "" {
		k.mu.Unlock()
		return []byte(leader + "\r\r")
	}
	// notify when command applied
	notify := make(chan result, 1)
	k.notifyChans.push(&item{term: term, index: index, notify: notify})
	k.mu.Unlock()
	res := <-notify
	if !res.success {
		return errorMessage
	}
	return okMessage
}

func (k *KVStore) submitDrop(cmd *Command) []byte {
	// Wrong command format
	if len(cmd.Bulks) != 2 {
		return errorMessage
	}
	// Lock here to avoid raft apply before notifyChans.push
	k.mu.Lock()
	// Send to raft
	leader, term, index := k.r.Submit(*cmd)
	// Return leader address
	if leader != "" {
		k.mu.Unlock()
		return []byte(leader + "\r\r")
	}
	// notify when command applied
	notify := make(chan result, 1)
	k.notifyChans.push(&item{term: term, index: index, notify: notify})
	k.mu.Unlock()
	res := <-notify
	if !res.success {
		return errorMessage
	}
	return okMessage
}

func (k *KVStore) submitSearchStudent(cmd *Command) []byte {
	// Wrong command format
	if len(cmd.Bulks) != 1 {
		return errorMessage
	}
	// Lock here to avoid raft apply before notifyChans.push
	k.mu.Lock()
	// Send to raft
	leader, term, index := k.r.Submit(*cmd)
	// Return leader address
	if leader != "" {
		k.mu.Unlock()
		return []byte(leader + "\r\r")
	}
	// notify when command applied
	notify := make(chan result, 1)
	k.notifyChans.push(&item{term: term, index: index, notify: notify})
	k.mu.Unlock()
	res := <-notify
	if !res.success {
		return errorMessage
	}
	// Format res.data
	// `*1\r\n$3\r\nnil\r\n`
	res.data = append([]byte("*1\r\n$"+strconv.FormatInt(int64(len(res.data)), 10)+"\r\n"), res.data...)
	res.data = append(res.data, CRLF...)
	res.data = append(res.data, []byte("\r\r")...)
	return res.data
}

func parseData(r *bufio.Reader) (*Command, error) {
	n := 1
	var (
		data    []byte
		err     error
		cmd     *Command
		advance int
	)
	for {
		_, err = r.Peek(n)
		if err != nil {
			break
		}
		data, err = r.Peek(r.Buffered())
		if err != nil {
			break
		}
		cmd, advance, err = tryParse(data)
		if cmd != nil || err != nil {
			break
		}
		n = r.Buffered() + 1
	}
	if cmd != nil {
		r.Discard(advance)
	}
	return cmd, err
}

var CRLF = []byte("\r\n")
var errBrokenMessage = errors.New("broken message")

func tryParse(data []byte) (*Command, int, error) {
	idx := bytes.Index(data, CRLF)
	if idx == -1 {
		return nil, -1, nil
	} else if data[0] != '*' {
		return nil, -1, errBrokenMessage
	}
	bulkCnt, err := strconv.Atoi(string(data[1:idx]))
	if err != nil || bulkCnt < 1 {
		return nil, -1, errBrokenMessage
	}
	d := data[idx+2:]
	bulks := make([]string, 0)
	for range bulkCnt {
		idx = bytes.Index(d, CRLF)
		// No CRLF
		if idx == -1 {
			return nil, -1, nil
		} else if d[0] != '$' {
			return nil, -1, errBrokenMessage
		}
		length, err := strconv.Atoi(string(d[1:idx]))
		// Invalid length
		if err != nil {
			return nil, -1, errBrokenMessage
		}
		d = d[idx+2:]
		// No sufficient data
		if len(d) < length+2 {
			return nil, -1, nil
		}
		bulks = append(bulks, string(d[:length]))
		d = d[length:]
		// No ending CRLF
		if !bytes.HasPrefix(d, CRLF) {
			return nil, -1, errBrokenMessage
		}
		d = d[2:]
	}
	cmd := &Command{}
	switch bulks[0] {
	case "SEARCHCOURSE":
		cmd.Cmd = CmdSearchCourse
	case "SEARCHALL":
		cmd.Cmd = CmdSearchAll
	case "SEARCHSTUDENT":
		cmd.Cmd = CmdSearchStudent
	case "DROP":
		cmd.Cmd = CmdDrop
	case "CHOOSE":
		cmd.Cmd = CmdChoose
	default:
		return nil, 1, errBrokenMessage
	}
	cmd.Bulks = bulks[1:]
	return cmd, len(data) - len(d), nil
}

// CAUTION !!!
// Just like raft generateId, this function is NOT safe
// Generate address for raft
// Port range 30000 ~ 40000
func (k *KVStore) generateAddrs(endpoints []string) (addrs []string) {
	for _, endpoint := range endpoints {
		idx := strings.IndexByte(endpoint, ':')
		prefix := endpoint[:idx+1]
		f := fnv.New32a()
		f.Write([]byte(endpoint))
		id := int(f.Sum32()) % 10000
		if id < 0 {
			id = -id
		}
		addrs = append(addrs, prefix+strconv.FormatInt(int64(id)+30000, 10))
	}
	return addrs
}
