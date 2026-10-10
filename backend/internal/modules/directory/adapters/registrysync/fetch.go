package registrysync

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const ANPURL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos/dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv"
const IBGEURL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=id"
const SourceCap = 12 << 20
const ReferenceCap = 8 << 20
const AliasCap = 4 << 20

func (r *Runner) download(ctx context.Context, url, path string, cap int64) error {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if e != nil {
		return fmt.Errorf("source request failed")
	}
	req.Header.Set("User-Agent", "Abastevo-registry-sync/1")
	req.Header.Set("Accept-Encoding", "gzip")
	// Copy the injected transport/timeout, but never permit a foreign redirect.
	client := *r.Client
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) >= 3 || next.URL.Host != req.URL.Host || next.URL.Scheme != "https" {
			return fmt.Errorf("source redirect refused")
		}
		return nil
	}
	response, e := client.Do(req)
	if e != nil {
		return fmt.Errorf("official source unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.ContentLength > cap {
		return fmt.Errorf("official source status/length refused")
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	wire := &io.LimitedReader{R: response.Body, N: cap + 1}
	var decoded io.Reader = wire
	switch response.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		zip, err := gzip.NewReader(wire)
		if err != nil {
			return fmt.Errorf("invalid source compression")
		}
		defer zip.Close()
		decoded = zip
	default:
		return fmt.Errorf("unsupported source compression")
	}
	n, e := io.Copy(f, io.LimitReader(decoded, cap+1))
	if e != nil || n == 0 || n > cap || wire.N <= 0 {
		return fmt.Errorf("official source incomplete or oversized")
	}
	return f.Sync()
}

type uf struct {
	Sigla string `json:"sigla"`
}
type region struct {
	UF uf `json:"UF"`
}
type municipality struct {
	ID    int    `json:"id"`
	Name  string `json:"nome"`
	Micro *struct {
		Meso region `json:"mesorregiao"`
	} `json:"microrregiao"`
	Immediate struct {
		Intermediate region `json:"regiao-intermediaria"`
	} `json:"regiao-imediata"`
}
type alias struct {
	UF      string   `json:"uf"`
	IBGE    string   `json:"ibge"`
	Aliases []string `json:"aliases"`
}

func aliases(dir string) error {
	f, e := os.Open(filepath.Join(dir, "ibge.json"))
	if e != nil {
		return e
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, ReferenceCap+1))
	token, e := d.Token()
	if e != nil || token != json.Delim('[') {
		return fmt.Errorf("invalid IBGE list")
	}
	entries := []alias{}
	seen := map[int]bool{}
	ufs := " AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO "
	for d.More() {
		var row municipality
		if e = d.Decode(&row); e != nil {
			return fmt.Errorf("invalid IBGE record")
		}
		state := row.Immediate.Intermediate.UF.Sigla
		if row.Micro != nil {
			state = row.Micro.Meso.UF.Sigla
		}
		code := strconv.Itoa(row.ID)
		if len(entries) >= 10000 || seen[row.ID] || len(code) != 7 || !strings.Contains(ufs, " "+state+" ") || state == "" || strings.TrimSpace(row.Name) == "" || len(row.Name) > 200 {
			return fmt.Errorf("invalid or duplicate IBGE identity")
		}
		seen[row.ID] = true
		entries = append(entries, alias{state, code, []string{row.Name}})
	}
	if token, e = d.Token(); e != nil || token != json.Delim(']') || len(entries) == 0 {
		return fmt.Errorf("incomplete IBGE list")
	}
	var tail any
	if e = d.Decode(&tail); e != io.EOF {
		return fmt.Errorf("trailing IBGE input")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].IBGE < entries[j].IBGE })
	b, e := json.Marshal(struct {
		Entries []alias `json:"entries"`
	}{entries})
	if e != nil || len(b) > AliasCap {
		return fmt.Errorf("alias reference exceeds bound")
	}
	return atomicFile(dir, "aliases.json", append(b, '\n'))
}
