package main

import (
    "os"
    "bufio"
    "bytes"
    "io"
    "fmt"
    "strings"
    "sort"
)

// Read a whole file into the memory and store it as array of lines
func readLines(path string, numLine int) (lines []string, err error) {
    var (
        file *os.File
        part []byte
        prefix bool
    )
    if file, err = os.Open(path); err != nil {
        return
    }
    defer file.Close()

    reader := bufio.NewReader(file)
    buffer := bytes.NewBuffer(make([]byte, 0))
    for i := 0; i < numLine; i++ {
        if part, prefix, err = reader.ReadLine(); err != nil {
            break
        }
        buffer.Write(part)
        if !prefix {
            lines = append(lines, buffer.String())
            buffer.Reset()
        }
    }
    if err == io.EOF {
        err = nil
    }
    return
}

func writeLines(lines []string, path string) (err error) {
    var (
        file *os.File
    )

    if file, err = os.Create(path); err != nil {
        return
    }
    defer file.Close()

    //writer := bufio.NewWriter(file)
    for _,item := range lines {
        //fmt.Println(item)
        _, err := file.WriteString(strings.TrimSpace(item) + "\n"); 
        //file.Write([]byte(item)); 
        if err != nil {
            //fmt.Println("debug")
            fmt.Println(err)
            break
        }
    }
    /*content := strings.Join(lines, "\n")
    _, err = writer.WriteString(content)*/
    return
}

func Found(s string, a []string) bool {
     //fmt.Println("DEBUG: %s\n %s\n %s \n", a, s, sort.SearchStrings(a, s))
     res := sort.SearchStrings(a, s) < len(a)
     //fmt.Println("DEBUG:", res)
     return res
}

func getSources(sourceLines []string, objectIdList []string) (lines []string) {
    for _, line := range sourceLines {
        objectIdForeignKey := strings.Split(line, ",")[3]
	if Found(objectIdForeignKey, objectIdList) {
	    lines = append(lines, line)
	}
    }
    return
}

func main() {

    var (
        objectFileName = "/space/fjammes/lsst/pt11/Object.txt"
	numLine=1000
	sourceFileName = "/space/fjammes/lsst/pt11/Source.txt"
	objectIdList []string = nil
    )
    lines, err := readLines(objectFileName,numLine)
    if err != nil {
        fmt.Println("Error: %s\n", err)
        return
    }
    
    sourceLines, err := readLines(sourceFileName,numLine * 100)
    if err != nil {
        fmt.Println("Error: %s\n", err)
        return
    }

    for _, line := range lines {
	objectIdList = append(objectIdList, strings.Split(line, ",")[0])
    }
    
    extractSourceLines := getSources(sourceLines,objectIdList)
    err = writeLines(lines, "Object.txt")
    err = writeLines(extractSourceLines, "Source.txt")
    fmt.Println(err)
}
