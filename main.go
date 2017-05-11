package main

import (
    "bufio"
    "encoding/base64"
    "flag"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/Azure/azure-sdk-for-go/storage"
)

const (
    simpleBlobMaxSize  = 64 * 1024 * 1024
    blockBlobChunkSize = 4 * 1024 * 1024
)

// config vars
var (
    account       string
    key           string
    container     string
    path          string
    workerCount   int
    maxRetries    int
    whitelistPath string
    blacklistPath string
)

var (
    blacklist    = make([]string, 0)
    workChan     = make(chan string, 0)
    fileCount    int
    errCount     int32
    successCount int32
    srv          storage.BlobStorageClient
    whitelist    = make([]string, 0)
    wg           sync.WaitGroup
)

func main() {
    parseArgs()

    fmt.Printf("Launching %d workers...\n", workerCount)
    for i := 0; i < workerCount; i++ {
        go runWorker()
    }

    fmt.Printf("Initialzing Azure...\n")
    cli, err := storage.NewBasicClient(account, key)
    if err != nil {
        panic(err)
    }

    srv = cli.GetBlobService()

    _, err = srv.CreateContainerIfNotExists(
        container,
        storage.ContainerAccessTypePrivate,
    )
    if err != nil {
        panic(err)
    }

    if len(whitelistPath) > 0 {
        f, err := os.Open(whitelistPath)
        if err != nil {
            panic(err)
        }
        defer f.Close()

        scanner := bufio.NewScanner(f)
        for scanner.Scan() {
            whitelist = append(whitelist, strings.TrimSpace(scanner.Text()))
        }
    }

    if len(blacklistPath) > 0 {
        f, err := os.Open(blacklistPath)
        if err != nil {
            panic(err)
        }
        defer f.Close()

        scanner := bufio.NewScanner(f)
        for scanner.Scan() {
            blacklist = append(blacklist, strings.TrimSpace(scanner.Text()))
        }
    }

    fmt.Printf("Enumerating files...\n")
    err = filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
        if err != nil {
            fmt.Println(err)
            return nil
        }

        if info.IsDir() {
            fmt.Printf("SKIP :: %s: IsDir\n", filePath)
            return nil
        }

        if isBlacklisted(filePath) {
            fmt.Printf("SKIP :: %s: Blacklisted\n", filePath)
            return nil
        }

        if !isWhitelisted(filePath) {
            fmt.Printf("SKIP :: %s: Unknown\n", filePath)
            return nil
        }

        fileCount++
        wg.Add(1)
        workChan <- filePath

        return nil
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Waiting for uploads to complete...\n")
    wg.Wait()

    close(workChan)

    fmt.Printf(
        "Run complete. %d files, %d success, %d errors\n",
        fileCount,
        successCount,
        errCount,
    )
}

func isBlacklisted(filePath string) bool {
    for i := range blacklist {
        if strings.Contains(filePath, blacklist[i]) {
            return true
        }
    }

    return false
}

func isWhitelisted(filePath string) bool {
    if len(whitelist) < 1 {
        return true
    }

    for i := range whitelist {
        if strings.Contains(filePath, whitelist[i]) {
            return true
        }
    }

    return false

}

func runWorker() {
    buffer := make([]byte, 4*1024*1024)

    for {
        select {
        case path, more := <-workChan:
            if !more {
                return
            }

            upload(path, buffer)
        }
    }
}

func upload(filePath string, buffer []byte) {
    defer wg.Done()

    var err error

    f, err := os.Open(filePath)
    if err != nil {
        reportError(filePath, err)
        return
    }
    defer f.Close()

    fi, err := f.Stat()
    if err != nil {
        reportError(filePath, err)
        return
    }

    targetPath := strings.Replace(filePath, path, "", -1)
    if len(targetPath) < 1 {
        return
    }

    if targetPath[0] == filepath.Separator {
        targetPath = targetPath[1:]
    }

    targetPath = strings.Replace(targetPath, "\\", "/", -1)

    if !shouldUpload(container, targetPath, fi.Size()) {
        fmt.Printf("SKIP :: %s :: Already exists\n", targetPath)
        return
    }

    if fi.Size() < simpleBlobMaxSize {
        err = simpleUpload(container, targetPath, f, fi.Size())
    } else {
        err = blockUpload(container, targetPath, f, buffer)
    }

    if err != nil {
        reportError(targetPath, err)
        return
    }

    fmt.Printf("SUCCESS :: %s\n", targetPath)
    atomic.AddInt32(&successCount, 1)
}

func simpleUpload(container, targetPath string, f *os.File, size int64) error {
    fmt.Printf("CREATE :: SIMPLE :: %s/%s\n", container, targetPath)

    reader := bufio.NewReaderSize(f, 64*1024)
    return srv.CreateBlockBlobFromReader(container, targetPath, uint64(size), reader, nil)
}

func blockUpload(container, targetPath string, f *os.File, buffer []byte) error {
    fmt.Printf("CREATE :: BLOCK :: %s/%s\n", container, targetPath)

    err := srv.CreateBlockBlob(container, targetPath)
    if err != nil {
        return err
    }

    blockList := make([]storage.Block, 0)

    for block := 0; ; block++ {
        c, err := f.Read(buffer)
        if err != nil || c < 1 {
            break
        }

        id := fmt.Sprintf("%05d", block)
        b64Id := base64.URLEncoding.EncodeToString([]byte(id))

        for i := 0; i < maxRetries; i++ {
            fmt.Printf("\tPutBlock :: %s (id %s, count %d, retry %d)\n", targetPath, b64Id, c, i)

            err = srv.PutBlock(container, targetPath, b64Id, buffer[:c])
            if err == nil {
                break
            }

            <-time.After(1 * time.Second)
        }

        if err != nil {
            return err
        }

        blockList = append(blockList, storage.Block{
            b64Id,
            storage.BlockStatusUncommitted,
        })
    }

    return srv.PutBlockList(container, targetPath, blockList)
}

func shouldUpload(container, targetPath string, size int64) bool {
    props, err := srv.GetBlobProperties(container, targetPath)
    if err != nil {
        fmt.Printf("UPLOAD :: %s/%s :: %v\n", container, targetPath, err)
        return true
    }

    if props.ContentLength != size {
        fmt.Printf(
            "UPLOAD :: %s/%s :: Size mismatch (%d != %d)\n",
            container,
            targetPath,
            props.ContentLength,
            size,
        )
        return true
    }

    return false
}

func reportError(filePath string, err error) {
    fmt.Printf("ERROR :: %s :: %v\n", filePath, err)
    atomic.AddInt32(&errCount, 1)
    return
}

func parseArgs() {
    flag.StringVar(&account, "Account", "", "The Azure storage account to use.")
    flag.StringVar(&key, "Key", "", "The secret key to use to authenticate to the Azure storage account.")
    flag.StringVar(&container, "Container", "", "The storage container to upload files to.")
    flag.StringVar(&path, "Path", ".", "The path to the directory of files to upload.")
    flag.IntVar(&workerCount, "Workers", 5, "Number of parallel uploaders to run.")
    flag.IntVar(&maxRetries, "MaxRetries", 3, "Number of retries to attempt on upload failure.")
    flag.StringVar(&whitelistPath, "Whitelist", "", "Path to a file containing whitelist entries.")
    flag.StringVar(&blacklistPath, "Blacklist", "", "Path to a file containing blacklist entries.")
    flag.Parse()
}
