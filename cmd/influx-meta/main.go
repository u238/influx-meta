package main

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"io/ioutil"
	"os"
	"time"
)

func main() {
	influxdbPath := "/var/lib/influxdb/"
	cfg := meta.NewConfig()
	cfg.Dir = influxdbPath + "meta/"

	dataPath := influxdbPath + "data/"

	c := meta.NewClient(cfg)

	if err := c.Open(); err != nil {
		panic(err)
	}
	fmt.Printf("%s", spew.Sdump(c))

	// c.Databases()[0].RetentionPolicy("default").ShardGroups

	files, err := ioutil.ReadDir(dataPath)
	if err != nil {
		fmt.Errorf("%s", err)
	}

	for _, dbdir := range files {
		// dig into databases
		dbInfo := c.Database(dbdir.Name())
		if dbInfo != nil {
			rpfiles, err := ioutil.ReadDir(dataPath + "/" + dbdir.Name())
			if err != nil {
				fmt.Errorf("%s", err)
			}
			for _, rpdir := range rpfiles {
				// dig into retention policies
				dbInfo.RetentionPolicy(rpdir.Name())
				rpi := dbInfo.RetentionPolicy(rpdir.Name())
				if rpi != nil {

				} else {
					fmt.Errorf("[!] Retention policy '%s' not found for database ", err)
				}
			}
		} else {
			fmt.Printf("database %s is missing in configuration", dbdir.Name())
		}
	}

	for _, db := range c.Databases() {
		fmt.Printf("%s\n", db.Name)
		for _, rp := range db.RetentionPolicies {
			fmt.Printf(" %s\n", rp.Name)

			for _, sg := range rp.ShardGroups {
				fmt.Printf("  %d\n", sg.ID)
			}
		}
	}

	os.Exit(0)
}

func getFilesInDir() {

}

func getMinMax(path string) (time.Time, time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("error opening TSM file: %s", err.Error())
	}
	defer r.Close()

	minTime, maxTime := r.TimeRange()

	return time.Unix(0, minTime).UTC(), time.Unix(0, maxTime).UTC(), nil
}

func getStartEnd(min time.Time, max time.Time) (time.Time, time.Time) {
	start := time.Date(min.Year(), min.Month(), min.Day(), 0, 0, 0, 0, min.Location())
	end := time.Date(max.Year(), max.Month(), max.Day(), 0, 0, 0, 0, max.Location()).AddDate(0, 0, 1)
	return start, end
}
