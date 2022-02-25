package snapshot

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func write(filePath string, offset int64, data *[]Entry) int64 {
	var csv string
	for _, entry := range *data {
		csv += strconv.Itoa(entry.Round) + "," + entry.CreatedAt.Format("2006-01-02T15:04:05.999999999Z07:00") + "," +
			entry.Data + "\n"
	}
	return appendFile(filePath, csv, offset)
}

func appendFile(filePath string, data string, offset int64) int64 {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return -1
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(file)
	written, err := file.WriteAt([]byte(data), offset)
	if err != nil {
		fmt.Println("what", err)
	}
	return offset + int64(written)
}

func read(filePath string) (*[]Entry, int64) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err.Error())
		return nil, 0
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(file)
	var lines []Entry
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		lineWords := strings.Split(scanner.Text(), ",")
		if len(lineWords) == 0 || len(lineWords) != 3 {
			fmt.Println("Bad len")
			continue
		}
		time, err := time.Parse(time.RFC3339Nano, lineWords[1])
		if err != nil {
			fmt.Println(err)
			continue
		}
		r, _ := strconv.Atoi(lineWords[0])
		lines = append(lines, Entry{
			Round:     r,
			CreatedAt: time,
			Data:      lineWords[2],
		})
	}
	f, _ := file.Stat()
	return &lines, f.Size()
}
func getSize(filePath string) int64 {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return 0
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(file)
	f, _ := file.Stat()
	return f.Size()
}
