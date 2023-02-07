package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	_ "github.com/joho/godotenv/autoload"
)

type Objects struct {
	url       string
	mime      string
	syncId    string
	encoding  string
	authToken string
}

func New() *Objects {
	url := os.Getenv("SEGMENT_OBJECTS_BULK_ENDPOINT")
	writeKey := os.Getenv("SEGMENT_WRITE_KEY")
	//writeKey = writeKey + ":"

	authToken := base64.StdEncoding.EncodeToString([]byte(writeKey))
	return &Objects{
		url:       url,
		mime:      "binary/octet-stream",
		encoding:  "gzip",
		authToken: authToken,
	}
}

type StartResponse struct {
	SyncId string `json:"sync_id"`
}

type PartResponse struct {
	PartId string `json:"part_id"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func (o *Objects) Start() {
	url := o.url + "/start"
	request, err := http.NewRequest(http.MethodPost, url, nil)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Basic "+o.authToken)

	if err != nil {
		log.Fatalln(err)
	}

	client := &http.Client{}

	response, err := client.Do(request)
	if err != nil {
		log.Fatal(err)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatalln(err)
	}

	sr := StartResponse{}
	err = json.Unmarshal(body, &sr)
	if err != nil {
		log.Fatalln(err)
	}
	o.syncId = sr.SyncId
	fmt.Println(o.syncId)
}

func (o *Objects) Upload() {
	file, err := os.Open("s3-data/jsonFiles/students.objects.json.gz")
	if err != nil {
		log.Fatalln(err)
	}

	url := o.url + "/upload/" + o.syncId
	fmt.Println(url)
	request, err := http.NewRequest(http.MethodPost, url, file)
	request.Header.Set("Content-Type", o.mime)
	request.Header.Set("Content-Encoding", o.encoding)
	request.Header.Set("Authorization", "Basic "+o.authToken)

	if err != nil {
		log.Fatalln(err)
	}

	client := &http.Client{}

	response, err := client.Do(request)
	if err != nil {
		log.Fatal(err)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatalln(err)
	}

	pr := PartResponse{}

	err = json.Unmarshal(body, &pr)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(pr.PartId)
}

func (o *Objects) Finish() {
	url := o.url + "/finish/" + o.syncId
	fmt.Println(url)
	rbody := "{\"error\":\"\"}"
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer([]byte(rbody)))
	request.Header.Set("Content-Type", "application/json	")
	request.Header.Set("Authorization", "Basic "+o.authToken)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(request)

	client := &http.Client{}

	response, err := client.Do(request)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(response.StatusCode)

	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%#v\n", string(body))
	er := ErrorResponse{}
	err = json.Unmarshal(body, &er)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(er.Error)
}
