package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/fabxc/tindex"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "tindex",
		Short: "CLI tool for tindex",
	}

	root.AddCommand(
		NewBenchCommand(),
	)

	root.Execute()
}

func NewBenchCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "bench",
		Short: "run benchmarks",
	}
	c.AddCommand(NewBenchWriteCommand())

	return c
}

func NewBenchWriteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "write <file>",
		Short: "run a write performance benchmark",
		Run:   writeBenchmark,
	}
}

func writeBenchmark(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		exitWithError(fmt.Errorf("missing file argument"))
	}

	var docs []*tindex.Doc

	measureTime("readData", func() {
		f, err := os.Open(args[0])
		if err != nil {
			exitWithError(err)
		}
		defer f.Close()

		docs, err = readPrometheusLabels(f)
		if err != nil {
			exitWithError(err)
		}
	})

	dir, err := ioutil.TempDir("", "tindex_bench")
	if err != nil {
		exitWithError(err)
	}
	ix, err := tindex.Open(dir, nil)
	if err != nil {
		exitWithError(err)
	}

	measureTime("indexData", func() {
		indexDocs(ix, docs, 100000)
	})
}

func indexDocs(ix *tindex.Index, docs []*tindex.Doc, batchSize int) {
	remDocs := docs[:]
	var ids []tindex.DocID

	for len(remDocs) > 0 {
		n := batchSize
		if n > len(remDocs) {
			n = len(remDocs)
		}

		b, err := ix.Batch()
		if err != nil {
			exitWithError(err)
		}
		for _, d := range remDocs[:n] {
			ids = append(ids, b.Index(d))
		}
		if err := b.Commit(); err != nil {
			exitWithError(err)
		}

		remDocs = remDocs[n:]
	}
}

func measureTime(stage string, f func()) {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	f()
	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
}

func readPrometheusLabels(r io.Reader) ([]*tindex.Doc, error) {
	dec := expfmt.NewDecoder(r, expfmt.FmtProtoText)

	var docs []*tindex.Doc
	var mf dto.MetricFamily

	for {
		if err := dec.Decode(&mf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		for _, m := range mf.GetMetric() {
			d := &tindex.Doc{
				Terms: make(tindex.Terms, len(m.GetLabel())+1),
				Body:  []byte("1234567890"),
			}
			d.Terms[0] = tindex.Term{
				Field: "__name__",
				Val:   mf.GetName(),
			}
			for i, l := range m.GetLabel() {
				d.Terms[i+1] = tindex.Term{
					Field: l.GetName(),
					Val:   l.GetValue(),
				}
			}
			docs = append(docs, d)
		}
	}

	return docs, nil
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
