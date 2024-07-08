package schema

type Course struct {
	CourseInfo
	Capacity int `json:"capacity"`
	Selected int `json:"selected"`
}

type Student struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type CourseInfo struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type Select struct {
	StuID    string `json:"stuid"`
	CourseId string `json:"course_id"`
}

type StudentCourse struct {
	Student
	Course []CourseInfo `json:"course"`
}
