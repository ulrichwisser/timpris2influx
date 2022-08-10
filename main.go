/*
Copyright © 2022 Ulrich Wisser

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	//	"github.com/spf13/pflag"

	"github.com/gocolly/colly"
	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	homedir "github.com/mitchellh/go-homedir"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

const VERBOSE = "verbose"
const VERBOSE_QUIET int = 0
const VERBOSE_ERROR int = 1
const VERBOSE_WARNING int = 2
const VERBOSE_INFO int = 3
const VERBOSE_DEBUG int = 4

var rootCmd = &cobra.Command{
	Use:   "timpris2influx",
	Short: "Save Swedish Power Prics to Influx DB",
	Long: `Save Swedish hour by hour power prices to influx db.`,
	Run: run,
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.Flags().CountP("verbose", "v", "repeat for more verbose printouts")
	rootCmd.Flags().StringP("config", "f", "", "config file (default is $HOME/.timpris)")

	// Use flags for viper values
	viper.BindPFlags(rootCmd.Flags())
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// Find home directory.
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Set defaults
	//
	// default log loglevel
	//		1	errors
	//		2	warnings
	//		3	info
	//		4	debug
	viper.SetDefault("verbose", 0)

	// Search config in home directory with name ".umpy" (without extension).
	viper.SetConfigName(".timpris")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(home)
	viper.AddConfigPath(".")

	// config file specified om command line
	if viper.GetString("config") != "" {
		// Use config file from the flag.
		viper.SetConfigFile(viper.GetString("config"))
	}

	// read in environment variables that match
	viper.SetEnvPrefix("TIMPRIS")
	viper.AutomaticEnv()

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		if viper.GetString("config") != "" {
			fmt.Println("Error reading config file.")
			fmt.Println(err)
		}
	} else {
		if viper.GetInt("verbose") >= VERBOSE_DEBUG {
			fmt.Println("Using config file:", viper.ConfigFileUsed())
		}
	}
}

type Timpris struct {
	Label       string
	Data        []float64
	Borderwidth int
	Fill        bool
	Bordercolor string
	Steppedline bool
}

func run(cmd *cobra.Command, args []string) {

	c := colly.NewCollector()
	c.OnRequest(func(r *colly.Request) {
		if viper.GetInt("verbose") >= VERBOSE_DEBUG {
			fmt.Println("Visiting", r.URL)
		}
	})
	c.OnHTML("canvas", func(e *colly.HTMLElement) {
		/*
			Decode labels first
			Check if labels are correct
			Decode data and write to influx
		*/

		// Decode labels
		var labels []int
		ld, err := base64.StdEncoding.DecodeString(e.Attr("data-labels"))
		if err != nil {
			return
		}
		err = json.Unmarshal(ld, &labels)
		if err != nil {
			return
		}

		// At this point we should be at the right data (power prices hour for hour)

		// Decode prices
		var prices []Timpris
		pd, err := base64.StdEncoding.DecodeString(e.Attr("data-datasets"))
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(pd, &prices)
		if err != nil {
			panic(err)
		}

		// At this we do have all data, only left to write it to influx

		// get access to Influx
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     viper.GetString("InfluxServer"),
			Username: viper.GetString("InfluxUser"),
			Password: viper.GetString("InfluxPasswd"),
		})
		if err != nil {
			panic(err)
		}
		defer c.Close()

		// Create the database handle, confirm driver is present
		var db *sql.DB
		var stmt *sql.Stmt
		var useMariadb bool = false
		if len(viper.GetString("MariaDSN"))>0 {
		db, _ = sql.Open("mysql", viper.GetString("MariaDSN"))
		defer db.Close()
		useMariadb = true
		stmt, err = db.Prepare("INSERT INTO pricesbyhour(year,month,day,hour,epoch,area,price) VALUES( ?, ?, ?, ?, ?, ?, ? )")
		if err != nil {
			panic(err)
		}
		defer stmt.Close() // Prepared statements take up server resources and should be closed after use.
		}
	
		// Create a new point batch
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  viper.GetString("InfluxDB"),
			Precision: "h",
		})
		if err != nil {
			panic(err)
		}

		// time reference
		year, month, day := time.Now().Date()

		for hour, price := range prices[0].Data {

			// tags
			tags := map[string]string{
				"hour": fmt.Sprintf("%d", hour),
				"area": "SE3",
			}

			// values
			fields := map[string]interface{}{
				"value": price,
			}

			// timestamp
			t := time.Date(year, month, day, hour, 0, 0, 0, time.Now().Location())

			// create new point
			pt, err := client.NewPoint("powerprices", tags, fields, t)
			if err != nil {
				panic(err)
			}

			// add point to list
			bp.AddPoint(pt)

			//
			if useMariadb {
				if _, err := stmt.Exec(year,month,day, hour, t.UTC().Format("2006-01-02 15:04:05"), "SE3", int(price*100)); err != nil {
					panic(err)
				}
		
			}
		}

		// Write the batch
		err = c.Write(bp)
		if err != nil {
			panic(err)
		}
	})
	c.Visit("https://elen.nu/dagens-spotpris/se3-stockholm/")
}
