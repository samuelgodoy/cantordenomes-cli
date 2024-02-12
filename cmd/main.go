package main

import (
    "bufio"
    "encoding/csv"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "time"

    "github.com/dgraph-io/badger/v3"
    "github.com/vbauerster/mpb/v5"
    "github.com/vbauerster/mpb/v5/decor"
    "cantordenomes/cantenomes"
)

type Decantado struct {
    ValorEncontrado string   `json:"encantado"`
    NomeDecantado   string   `json:"decantado"`
    Sexo            string   `json:"sexo"`
    Nascimento      string   `json:"nascimento"`
}

const (
    defaultDBName = "continente.db"
    batchSize     = 10000 // Número de registros para escrever no banco de dados
    numWorkers    = 50     // Número de workers para processamento paralelo
)

var (
    dbnameFlag     string
    bucketrootFlag string
    db             *badger.DB
    recordsCount   int64 // Contador de registros lidos
)

type linhaData struct {
    bucketName string
    resultado  []string
    sexo       string
    nasc       string
}

func main() {
    filenomescomuns := flag.String("inputname", "", "csv com nomes comuns")
    idchave := flag.String("idchave", "", "ID/Chave")
    filenomesfull := flag.String("fullnames", "", "csv com nomes completos")
    flag.StringVar(&dbnameFlag, "dbname", defaultDBName, "Database name")
    operation := flag.String("op", "", "operation (cantar)")
    flag.Parse()

    dbPath := filepath.Join(bucketrootFlag, dbnameFlag)

    switch *operation {
    case "cantar":
        if err := openDB(dbPath); err != nil {
            log.Fatalf("Erro ao abrir o banco de dados: %v", err)
        }
        defer db.Close()

        err := processCantarOperation(*filenomesfull, *filenomescomuns, dbPath)
        if err != nil {
            log.Fatalf("Erro ao realizar a operação 'cantar': %v", err)
        }
    case "decantar":
        if *idchave == "" {
            log.Fatal("ID/Chave não especificada para a operação decantar")
        }
        if err := openDB(dbPath); err != nil {
            log.Fatalf("Erro ao abrir o banco de dados: %v", err)
        }
        defer db.Close()
    
        if err := processDecantarOperation(*idchave,*filenomescomuns); err != nil {
            log.Fatalf("Erro ao realizar a operação 'decantar': %v", err)
        }


    case "init":
        if err := openDB(dbPath); err != nil {
            log.Fatalf("Erro ao abrir o banco de dados: %v", err)
        }
        defer db.Close()

    default:
        fmt.Print("Revise as operações")
    }
}

func processDecantarOperation(idchave,filenomescomuns string) error {
    //fmt.Println("Decantando valor para a chave:", idchave)

    return db.View(func(txn *badger.Txn) error {
        item, err := txn.Get([]byte(idchave))
        if err != nil {
            return fmt.Errorf("erro ao buscar a chave %s: %v", idchave, err)
        }

        return item.Value(func(val []byte) error {
            valSlice := strings.Split(string(val), "|")
            nomecantado := strings.Split(valSlice[0], " ")
            nome, _ := cantenomes.Decantar(nomecantado,&filenomescomuns)
            dados := Decantado{
                ValorEncontrado: string(val),
                NomeDecantado:   nome,
                Sexo:            valSlice[1],
                Nascimento:      valSlice[2],
            }
            jsonBytes, err := json.MarshalIndent(dados, "", "    ")
            if err != nil {
                fmt.Println("Erro ao gerar JSON:", err)
                return err
            }
            
            // Imprime o JSON formatado
            fmt.Println(string(jsonBytes))
            return nil
        })
    })
}

func openDB(dbPath string) error {
    var err error

    // Configura as opções do BadgerDB
    opts := badger.DefaultOptions(dbPath)

    // Desativa o logger do BadgerDB
    opts = opts.WithLogger(nil)

    // Abre o banco de dados com as opções configuradas
    db, err = badger.Open(opts)
    if err != nil {
        return err
    }

    // Como o logger foi desativado, essa mensagem não será exibida no console,
    // mas você pode mantê-la se desejar registrar em outro lugar.
    //fmt.Printf("Database '%s' opened successfully.\n", dbPath)
    return nil
}


func processCantarOperation(fullNamesFile, commonNamesFile, dbPath string) error {
    file, err := os.Open(fullNamesFile)
    if err != nil {
        return fmt.Errorf("Erro ao abrir o arquivo: %v", err)
    }
    defer file.Close()

    reader := csv.NewReader(bufio.NewReader(file))
    _, err = reader.Read() // Ler o cabeçalho
    if err != nil {
        return fmt.Errorf("Erro ao ler o cabeçalho: %v", err)
    }

    totalRecords, err := getLineCount(fullNamesFile)
    if err != nil {
        return err
    }

    p := mpb.New(
        mpb.WithWidth(80),
        mpb.WithRefreshRate(100*time.Millisecond),
    )

	bar := p.AddBar(int64(totalRecords),
    mpb.BarRemoveOnComplete(),
    mpb.PrependDecorators(
        decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
    ),
    mpb.AppendDecorators(
        decor.Percentage(decor.WC{W: 5}),
        decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO, decor.WC{W: 4}), "Tempo estimado: %s"),
        decor.OnComplete(decor.Elapsed(decor.ET_STYLE_GO, decor.WC{W: 4}), "Tempo percorrido: %s"),
    ),
)

    startTime := time.Now()

    linesChan := make(chan []string)
    errChan := make(chan error, 1)
    var wg sync.WaitGroup

    // Iniciar workers
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go worker(linesChan, errChan, &wg, commonNamesFile, dbPath, bar)
    }

    go func() {
        for {
            linha, err := reader.Read()
            if err != nil {
                if err == io.EOF {
                    break // Fim do arquivo
                }
                errChan <- fmt.Errorf("Erro ao ler a linha: %v", err)
                return
            }
            linesChan <- linha
        }
        close(linesChan)
    }()

    go func() {
        wg.Wait()
        close(errChan)
    }()

    for err := range errChan {
        return err
    }

    p.Wait()
    elapsedTime := time.Since(startTime)
    estimatedTime := time.Duration(recordsCount/batchSize) * elapsedTime
    fmt.Printf("Tempo estimado para processar todos os registros: %s\n", estimatedTime)

    return nil
}

func worker(linesChan <-chan []string, errChan chan<- error, wg *sync.WaitGroup, commonNamesFile, dbPath string, bar *mpb.Bar) {
    defer wg.Done()

    for linha := range linesChan {
        resultado := cantenomes.Cantar(linha[1], linha[0], commonNamesFile)
        bucketName := linha[2]
        nasc := strings.Replace(linha[3], "-", "", -1)
        valorConcatenado := strings.Join([]string{strings.Join(resultado, " "), linha[1], nasc}, "|")
        chave := []byte(bucketName)

        if err := putRecord(chave, []byte(valorConcatenado)); err != nil {
            errChan <- fmt.Errorf("Erro ao inserir dados no banco de dados: %v", err)
            return
        }
        recordsCount++
        bar.IncrBy(1)
    }
}

func getLineCount(filename string) (int64, error) {
    file, err := os.Open(filename)
    if err != nil {
        return 0, err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    var lineCount int64

    for scanner.Scan() {
        lineCount++
    }

    return lineCount, scanner.Err()
}

func putRecord(key []byte, value []byte) error {
    err := db.Update(func(txn *badger.Txn) error {
        return txn.Set(key, value)
    })
    return err
}