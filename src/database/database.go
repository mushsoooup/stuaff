package database

import (
	"bufio"
	"bytes"
	"cs/schema"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"
)

var kvs []string

func Init(addr []string) {
	kvs = addr
}

func process(text string) (string, error) {
	if text[0] == '-' {
		return "", errors.New("error response")
	} else if text[0] == '+' {
		return "", nil
	} else if text[0] != '*' {
		return text, nil
	}
	return "", nil
}

// `*1\r\n$3\r\nA02\r\n`
func SearchCourse(id string) (*schema.Course, error) {
	msg := fmt.Sprintf("*2\r\n$12\r\nSEARCHCOURSE\r\n$%d\r\n%s\r\n", len(id), id)
	kvs_idx := 0
	addr := kvs[kvs_idx]
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			kvs_idx = (kvs_idx + 1) % len(kvs)
			addr = kvs[kvs_idx]
			continue
		}
		conn.SetWriteDeadline(time.Now().Add(time.Millisecond * 200))
		conn.Write([]byte(msg))
		scanner := bufio.NewScanner(conn)
		scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}
			if i := bytes.Index(data, []byte("\r\r")); i >= 0 {
				return i + 2, data[0:i], nil
			}
			if atEOF {
				return len(data), data, nil
			}
			return 0, nil, nil
		})
		if !scanner.Scan() {
			return nil, errors.New("connection failed")
		}
		text := scanner.Text()
		addrRes, err := process(text)
		if err != nil {
			return nil, err
		} else if addrRes != "" {
			addr = addrRes
			continue
		}
		var courseStr string
		var courseLen int
		fmt.Sscanf(text, "*1\r\n$%d\r\n%s\r\n", &courseLen, &courseStr)
		var course schema.Course
		err = json.Unmarshal([]byte(courseStr), &course)
		if err != nil {
			return nil, err
		}
		return &course, nil
	}
}

func SearchAll() ([]schema.Course, error) {
	msg := "*1\r\n$9\r\nSEARCHALL\r\n"
	kvs_idx := 0
	addr := kvs[kvs_idx]
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			kvs_idx = (kvs_idx + 1) % len(kvs)
			addr = kvs[kvs_idx]
			continue
		}
		conn.SetWriteDeadline(time.Now().Add(time.Millisecond * 200))
		conn.Write([]byte(msg))
		scanner := bufio.NewScanner(conn)
		scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}
			if i := bytes.Index(data, []byte("\r\r")); i >= 0 {
				return i + 2, data[0:i], nil
			}
			if atEOF {
				return len(data), data, nil
			}
			return 0, nil, nil
		})
		if !scanner.Scan() {
			return nil, errors.New("connection failed")
		}
		text := scanner.Text()
		addrRes, err := process(text)
		if err != nil {
			return nil, err
		} else if addrRes != "" {
			addr = addrRes
			continue
		}
		var courseStr string
		var courseLen int
		fmt.Sscanf(text, "*1\r\n$%d\r\n%s\r\n", &courseLen, &courseStr)
		var courses []schema.Course
		err = json.Unmarshal([]byte(courseStr), &courses)
		if err != nil {
			return nil, err
		}
		return courses, nil
	}
}

func Choose(stuId, courseId string) error {
	msg := fmt.Sprintf("*3\r\n$6\r\nCHOOSE\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(stuId), stuId, len(courseId), courseId)
	kvs_idx := 0
	addr := kvs[kvs_idx]
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			kvs_idx = (kvs_idx + 1) % len(kvs)
			addr = kvs[kvs_idx]
			continue
		}
		conn.SetWriteDeadline(time.Now().Add(time.Millisecond * 200))
		conn.Write([]byte(msg))
		scanner := bufio.NewScanner(conn)
		scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}
			if i := bytes.Index(data, []byte("\r\r")); i >= 0 {
				return i + 2, data[0:i], nil
			}
			if atEOF {
				return len(data), data, nil
			}
			return 0, nil, nil
		})
		if !scanner.Scan() {
			return errors.New("connection failed")
		}
		text := scanner.Text()
		addrRes, err := process(text)
		if err != nil {
			return err
		} else if addrRes != "" {
			addr = addrRes
			continue
		}
		return nil
	}
}

func SearchStudent(id string) (*schema.StudentCourse, error) {
	msg := fmt.Sprintf("*2\r\n$13\r\nSEARCHSTUDENT\r\n$%d\r\n%s\r\n", len(id), id)
	kvs_idx := 0
	addr := kvs[kvs_idx]
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			kvs_idx = (kvs_idx + 1) % len(kvs)
			addr = kvs[kvs_idx]
			continue
		}
		conn.SetWriteDeadline(time.Now().Add(time.Millisecond * 200))
		conn.Write([]byte(msg))
		scanner := bufio.NewScanner(conn)
		scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}
			if i := bytes.Index(data, []byte("\r\r")); i >= 0 {
				return i + 2, data[0:i], nil
			}
			if atEOF {
				return len(data), data, nil
			}
			return 0, nil, nil
		})
		if !scanner.Scan() {
			return nil, errors.New("connection failed")
		}
		text := scanner.Text()
		addrRes, err := process(text)
		if err != nil {
			return nil, err
		} else if addrRes != "" {
			addr = addrRes
			continue
		}
		var studentStr string
		var studentLen int
		fmt.Sscanf(text, "*1\r\n$%d\r\n%s\r\n", &studentLen, &studentStr)
		var student schema.StudentCourse
		err = json.Unmarshal([]byte(studentStr), &student)
		if err != nil {
			return nil, err
		}
		return &student, nil
	}
}

func Drop(stuId, courseId string) error {
	msg := fmt.Sprintf("*3\r\n$4\r\nDROP\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(stuId), stuId, len(courseId), courseId)
	kvs_idx := 0
	addr := kvs[kvs_idx]
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			kvs_idx = (kvs_idx + 1) % len(kvs)
			addr = kvs[kvs_idx]
			continue
		}
		conn.SetWriteDeadline(time.Now().Add(time.Millisecond * 200))
		conn.Write([]byte(msg))
		scanner := bufio.NewScanner(conn)
		scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}
			if i := bytes.Index(data, []byte("\r\r")); i >= 0 {
				return i + 2, data[0:i], nil
			}
			if atEOF {
				return len(data), data, nil
			}
			return 0, nil, nil
		})
		if !scanner.Scan() {
			return errors.New("connection failed")
		}
		text := scanner.Text()
		addrRes, err := process(text)
		if err != nil {
			return err
		} else if addrRes != "" {
			addr = addrRes
			continue
		}
		return nil
	}
}
