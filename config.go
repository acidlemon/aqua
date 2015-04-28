package aqua

import (
	"bytes"
	"fmt"
	"net/url"
)

type Config struct {
	Host     string `yaml:"host" json:"host"`
	Port     string `yaml:"port" json:"port"`
	Socket   string `yaml:"socket" json:"socket"`
	UserName string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
	TLS      bool   `yaml:"tls" json:"tls"`
}

func (c *Config) DSN(driver string) string {
	dsn := ""
	params := map[string]string{}

	switch driver {
	case "mysql": // for github.com/go-mysql-driver/mysql
		if len(c.Socket) > 0 {
			dsn = fmt.Sprintf("%s:%s@unix(%s)/%s",
				c.UserName, c.Password, c.Socket, c.Database)
		} else {
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				c.UserName, c.Password, c.Host, c.Port, c.Database)
		}
		if c.TLS {
			params["tls"] = "true"
		}

	case "postgres": // for github.com/lib/pq
		if len(c.Socket) > 0 {
			// TODO because pq driver requires unix socket path as host/port pair
		} else {
			dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
				url.QueryEscape(c.UserName), url.QueryEscape(c.Password),
				url.QueryEscape(c.Host), c.Port, url.QueryEscape(c.Database))
		}
		if c.TLS {
			params["sslmode"] = "verify-full"
		}

		// TODO sqlite

	}

	if len(params) > 0 {
		buffer := bytes.Buffer{}
		buffer.WriteString("?")
		for k, v := range params {
			buffer.WriteString(url.QueryEscape(k))
			if len(v) > 0 {
				buffer.WriteString("=")
				buffer.WriteString(url.QueryEscape(v))
			}
		}

		dsn += buffer.String()
	}

	return dsn
}
