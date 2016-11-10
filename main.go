package main

import (
    "bufio"
    "flag"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "sync/atomic"
    "time"

    "github.com/Azure/azure-sdk-for-go/storage"
)

// config vars
var (
    account     string
    key         string
    container   string
    glob        string
    workerCount int
    maxRetries  int
)

var (
    workChan     = make(chan string, 0)
    fileCount    int
    errCount     int32
    successCount int32
    srv          storage.BlobStorageClient
    wg           sync.WaitGroup
)

func main() {
    parseArgs()

    for i := 0; i < workerCount; i++ {
        go runWorker()
    }

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

    matches, err := filepath.Glob(glob)
    if err != nil {
        panic(err)
    }

    fileCount = len(matches)
    wg.Add(fileCount)

    for i := range matches {
        workChan <- matches[i]
    }

    wg.Wait()

    fmt.Printf(
        "Run complete. %d files, %d success, %d errors\n",
        fileCount,
        successCount,
        errCount,
    )
}

func runWorker() {
    for {
        select {
        case path := <-workChan:
            fmt.Printf("Uploading %s\n", path)
            upload(path)
        }
    }
}

func upload(path string) {
    defer wg.Done()

    var exists bool
    var err error

    f, err := os.Open(path)
    if err != nil {
        reportError(path, err)
        return
    }
    defer f.Close()

    fi, err := f.Stat()
    if err != nil {
        reportError(path, err)
        return
    }

    reader := bufio.NewReaderSize(f, 64*1024)

    for i := 0; i < maxRetries; i++ {
        exists, err = srv.BlobExists(container, path)
        if err != nil {
            continue
        }

        if exists {
            break
        }

        err = srv.CreateBlockBlobFromReader(
            container,
            path,
            uint64(fi.Size()),
            reader,
            nil,
        )

        if err == nil {
            break
        }

        <-time.After(1 * time.Second)
    }

    if err != nil {
        reportError(path, err)
    } else {
        fmt.Printf("SUCCESS :: %s\n", path)
        atomic.AddInt32(&successCount, 1)
    }
}

func reportError(path string, err error) {
    fmt.Printf("ERROR :: %s :: %v\n", path, err)
    atomic.AddInt32(&errCount, 1)
    return
}

func parseArgs() {
    flag.StringVar(&account, "Account", "", "The Azure storage account to use.")
    flag.StringVar(&key, "Key", "", "The secret key to use to authenticate to the Azure storage account.")
    flag.StringVar(&container, "Container", "", "The storage container to upload files to.")
    flag.StringVar(&glob, "Path", "./*", "A path glob specifying which files to copy.")
    flag.IntVar(&workerCount, "Workers", 5, "Number of parallel uploaders to run.")
    flag.IntVar(&maxRetries, "MaxRetries", 3, "Number of retries to attempt on upload failure.")
    flag.Parse()
}
