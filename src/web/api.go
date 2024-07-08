package main

import (
	"cs/database"
	"cs/server"
	"encoding/json"
)

type Response struct {
	Code int    `json:"code"`
	Data any    `json:"data"`
	Msg  string `json:"msg"`
}

func GetCourse(ctx *server.RequestCtx) error {
	courseId := ctx.Req.GetParam("course_id")
	if courseId == "" {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  "invalid course_id",
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	course, err := database.SearchCourse(courseId)
	if err != nil {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  err.Error(),
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	ctx.Res.SetContentType("application/json;charset=utf-8")
	Response, _ := json.Marshal(Response{
		Code: 200,
		Data: course,
		Msg:  "ok",
	})
	ctx.Res.SetData(Response, 200)
	return nil
}

func GetAllCourse(ctx *server.RequestCtx) error {
	course, err := database.SearchAll()
	if err != nil {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  err.Error(),
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	ctx.Res.SetContentType("application/json")
	Response, _ := json.Marshal(Response{
		Code: 200,
		Data: course,
		Msg:  "ok",
	})
	ctx.Res.SetData(Response, 200)
	return nil
}

func GetStudent(ctx *server.RequestCtx) error {
	studentId := ctx.Req.GetParam("student_id")
	if studentId == "" {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  "invalid student_id",
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	sstudent, err := database.SearchStudent(studentId)
	if err != nil {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  err.Error(),
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	ctx.Res.SetContentType("application/json;charset=utf-8")
	Response, _ := json.Marshal(Response{
		Code: 200,
		Data: sstudent,
		Msg:  "ok",
	})
	ctx.Res.SetData(Response, 200)
	return nil
}

type ChooseReq struct {
	Stuid     string `json:"stuid"`
	Course_id string `json:"course_id"`
}

func Choose(ctx *server.RequestCtx) error {
	var req ChooseReq
	err := json.Unmarshal(ctx.Req.GetData(), &req)
	if err != nil {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  err.Error(),
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	if req.Stuid == "" {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  "invalid student_id",
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	if req.Course_id == "" {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  "invalid course_id",
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	err = database.Choose(req.Stuid, req.Course_id)
	if err != nil {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  err.Error(),
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	ctx.Res.SetContentType("application/json")
	data, _ := json.Marshal(Response{
		Code: 200,
		Data: nil,
		Msg:  "ok",
	})
	ctx.Res.SetData(data, 200)
	return nil
}

func Drop(ctx *server.RequestCtx) error {
	var req ChooseReq
	err := json.Unmarshal(ctx.Req.GetData(), &req)
	if err != nil {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  err.Error(),
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	if req.Stuid == "" {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  "invalid student_id",
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	if req.Course_id == "" {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  "invalid course_id",
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	err = database.Drop(req.Stuid, req.Course_id)
	if err != nil {
		ctx.Res.SetContentType("application/json")
		data, _ := json.Marshal(Response{
			Code: 200,
			Data: nil,
			Msg:  err.Error(),
		})
		ctx.Res.SetData(data, 200)
		return nil
	}
	ctx.Res.SetContentType("application/json")
	data, _ := json.Marshal(Response{
		Code: 200,
		Data: nil,
		Msg:  "ok",
	})
	ctx.Res.SetData(data, 200)
	return nil
}

func Ping(ctx *server.RequestCtx) error {
	ctx.Res.SetData([]byte("pong"), 200)
	return nil
}
