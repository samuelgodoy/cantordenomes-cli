package cantenomes

import (
    "encoding/csv"
    "fmt"
    "io"
    "math/big"
    "sync"
    "runtime"
    "math"
    "os"
    "strconv"
    "strings"
)

func Decantar(nomeModificado []string, inputname *string) (string, error) {
    // Abre o arquivo CSV
    arquivoCSV, err := os.Open(*inputname)
    if err != nil {
        return "", fmt.Errorf("erro ao abrir o arquivo: %v", err)
    }
    defer arquivoCSV.Close()

    leitorCSV := csv.NewReader(arquivoCSV)
    leitorCSV.Comma = ','

    // Lê todas as linhas do arquivo CSV
    linhasCSV, err := leitorCSV.ReadAll()
    if err != nil {
        return "", fmt.Errorf("erro ao ler o arquivo CSV: %v", err)
    }

    var palavras []string
    for _, valor := range nomeModificado {
        if IsNumeric(valor) {
            z, ok := new(big.Int).SetString(valor, 10)
            if !ok {
                return "", fmt.Errorf("erro ao converter %s para big.Int", valor)
            }
            x, y := cantorPairInverse(z)
            indiceX := x.Int64()
            indiceY := y.Int64()

            // Verifica se os índices estão dentro dos limites do CSV
            if int(indiceX) < len(linhasCSV) && int(indiceY) < len(linhasCSV) {
                palavras = append(palavras, linhasCSV[indiceX][0], linhasCSV[indiceY][0])
            } else {
                return "", fmt.Errorf("índice fora dos limites")
            }
        } else {
            // Caso não seja um número, apenas adiciona à lista de palavras
            palavras = append(palavras, valor)
        }
    }

    return strings.Join(palavras, " "), nil
}

func AbrirArquivoCSV(inputname string) (*os.File, error) {
    // Abrir o arquivo CSV.
    arquivoCSV, err := os.Open(inputname)
    if err != nil {
        return nil, err
    }
    return arquivoCSV, nil
}

// LerLinhasCSV lê um arquivo CSV aberto e retorna suas linhas como uma matriz de strings.
func LerLinhasCSV(arquivoCSV *os.File) ([][]string, error) {
    leitorCSV := csv.NewReader(arquivoCSV)
    leitorCSV.Comma = ','

    var linhasCSV [][]string
    for {
        linha, err := leitorCSV.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }
        linhasCSV = append(linhasCSV, linha)
    }
    return linhasCSV, nil
}


func SubstituirNomesPorIndices(palavras []string, linhasCSV [][]string) []string {
    var wg sync.WaitGroup
    numWorkers := runtime.NumCPU() // Limitando o número de goroutines ao número de CPUs
    canal := make(chan [2]int, numWorkers) // Ajustando o buffer do canal

    worker := func(start, end int) {
        defer wg.Done()
        for i := start; i < end; i++ {
            palavra := palavras[i]
            for indice, linha := range linhasCSV {
                if strings.EqualFold(linha[0], palavra) {
                    canal <- [2]int{i, indice}
                    return
                }
            }
            canal <- [2]int{i, -1}
        }
    }

    chunkSize := (len(palavras) + numWorkers - 1) / numWorkers
    for i := 0; i < len(palavras); i += chunkSize {
        end := i + chunkSize
        if end > len(palavras) {
            end = len(palavras)
        }
        wg.Add(1)
        go worker(i, end)
    }

    go func() {
        wg.Wait()
        close(canal)
    }()

    resultados := make([]string, len(palavras))
    for par := range canal {
        if par[1] == -1 {
            resultados[par[0]] = palavras[par[0]]
        } else {
            resultados[par[0]] = fmt.Sprintf("%d", par[1])
        }
    }

    return resultados
}


func ProcessarPalavras(palavras []string, linhasCSV [][]string) []string {
    resultadoFinal := make([]string, 0, len(palavras)) // Pré-alocação com base no tamanho de palavras

    parseToInt := func(s string) (int64, error) { // Função auxiliar para parsing
        return strconv.ParseInt(s, 10, 64)
    }

    for i := 0; i < len(palavras); {
        if i+1 < len(palavras) {
            if indice1, err := parseToInt(palavras[i]); err == nil {
                if indice2, err := parseToInt(palavras[i+1]); err == nil {
                    cantorZ := CantorPair(indice1, indice2)
                    resultadoFinal = append(resultadoFinal, strconv.FormatInt(cantorZ, 10))
                    i += 2
                    continue
                }
            }
        }

        if indice, err := parseToInt(palavras[i]); err == nil {
            if indice >= 0 && indice < int64(len(linhasCSV)) {
                resultadoFinal = append(resultadoFinal, linhasCSV[indice][0])
            }
        } else {
            resultadoFinal = append(resultadoFinal, palavras[i])
        }
        i++
    }
    return resultadoFinal
}



func Cantar(cpf string, nomeCompleto string, inputname string) []string {
    palavras := strings.Split(strings.TrimSpace(nomeCompleto), " ")
    
    arquivoCSV, err := AbrirArquivoCSV(inputname)
    if err != nil {
        fmt.Println("Erro ao abrir o arquivo:", err)
        return nil
    }
    defer arquivoCSV.Close()

    linhasCSV, err := LerLinhasCSV(arquivoCSV)
    if err != nil {
        fmt.Println("Erro ao ler o arquivo CSV:", err)
        return nil
    }

    palavrasComIndices := SubstituirNomesPorIndices(palavras, linhasCSV)
    return ProcessarPalavras(palavrasComIndices, linhasCSV)
}

func CantorPair(x, y int64) int64 {
    // Implementação da função Cantor Pair com int64
    w := (x + y) * (x + y + 1) / 2
    z := w + y
    return z
}

func cantorPairInverse(z *big.Int) (*big.Int, *big.Int) {
    // w = floor((sqrt(8*z + 1) - 1) / 2)
    // t = (w*w + w) / 2
    // y = z - t
    // x = w - y
    w := big.NewInt(0)
    t := big.NewInt(0)
    y := big.NewInt(0)
    x := big.NewInt(0)

    w.Mul(big.NewInt(8), z)
    w.Add(w, big.NewInt(1))
    w.Sqrt(w)
    w.Sub(w, big.NewInt(1))
    w.Div(w, big.NewInt(2))

    t.Mul(w, w)
    t.Add(t, w)
    t.Div(t, big.NewInt(2))

    y.Sub(z, t)
    x.Sub(w, y)

    return x, y
}

// isNumeric verifica se uma string é um número

func IsNumeric(s string) bool {
    f, err := strconv.ParseFloat(s, 64)
    if err != nil {
        return false // A conversão falhou, não é um valor numérico.
    }
    return !math.IsNaN(f)
}