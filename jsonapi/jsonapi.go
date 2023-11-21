package jsonapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

func setJsonHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

func fromJson[T any](body io.Reader, target T) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(body)
	json.Unmarshal(buf.Bytes(), target)
}

func returnJson[T any](w http.ResponseWriter, withData func() (T, error)) {
	setJsonHeader(w)

	data, err := withData()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		serverErrJosn, err := json.Marshal(&err)

		if err != nil {
			log.Println(err)
			return
		}

		w.Write(serverErrJosn)
		return
	}

	json, err := json.Marshal(&data)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(json)
}

func returnErr(w http.ResponseWriter, err error, code int) {
	returnJson(w, func() (interface{}, error) { // Using interface{} so we can return any type
		errorMessage := struct {
			Err string `json:"error"`
		}{
			Err: err.Error(),
		}
		w.WriteHeader(code)
		return errorMessage, nil
	})
}
