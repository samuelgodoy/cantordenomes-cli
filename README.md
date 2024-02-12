# Projeto de Nomes

Este projeto permite realizar operações de "cantar" e "decantar" nomes a partir de arquivos CSV e uma base de dados BadgerDB utilizando função matemática do Georg Cantor.

## Comandos

### Cantar

Para cantar nomes e inseri-los na base de dados, use o comando abaixo:

```bash
go run .\cmd\main.go -op=cantar -inputname .\nomes-comuns.csv -fullnames .\fake-nomes-seed.csv -dbname='nomes.db'
```

### Decantar

Para decantar um nome especificado pela sua chave ID no banco de dados, use o comando a seguir:

```bash
go run .\cmd\main.go -op=decantar -idchave=465807151 -inputname .\nomes-comuns.csv -dbname='nomes.db'
```

## Resultado da Decantação

O resultado do processo de decantação será similar ao seguinte JSON, demonstrando o nome encantado e decantado, juntamente com o sexo e a data de nascimento:

```json
{
    "encantado": "170596244 EPIFANIO|F|19230503",
    "decantado": "MARIA APARECIDA EPIFANIO",
    "sexo": "F",
    "nascimento": "19230503"
}
```
