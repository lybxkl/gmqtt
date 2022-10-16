package gcfg

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/go-playground/locales/zh"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	zh_translations "github.com/go-playground/validator/v10/translations/zh"

	"github.com/lybxkl/gmqtt/util"
)

//go:embed config.toml
var CfgFile []byte

var (
	gConfig  *GConfig
	Validate = validator.New()
	trans    ut.Translator
)

func init() {
	uni := ut.New(zh.New())
	trans, _ = uni.GetTranslator("zh")

	//注册一个函数，获取struct tag里自定义的label作为字段名
	Validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		label := fld.Tag.Get("label")
		if label == "" {
			return fld.Name
		}
		return label
	})

	util.MustPanic(Validate.RegisterValidation("default", func(fl validator.FieldLevel) bool {
		switch fl.Field().Kind() {
		case reflect.String:
			if fl.Field().String() == "" {
				if strings.Contains(fl.Param(), "*") {
					fl.Field().Set(reflect.ValueOf(strings.Replace(fl.Param(), "*", util.Generate(), 1)))
				} else {
					fl.Field().Set(reflect.ValueOf(fl.Param()))
				}
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if fl.Field().Int() == 0 {
				setIntOrUint(fl)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if fl.Field().Uint() == 0 {
				setIntOrUint(fl)
			}
		case reflect.Float32, reflect.Float64:
			if fl.Field().Float() == 0 {
				setFloat(fl)
			}
		}
		return true
	}))

	//验证器注册翻译器
	util.MustPanic(zh_translations.RegisterDefaultTranslations(Validate, trans))
}
func GetGCfg() *GConfig {
	return gConfig
}

func init() {
	if len(CfgFile) == 0 {
		panic(errors.New("not found config.toml"))
	}
	gConfig = &GConfig{}
	gConfig.Broker.RetainAvailable = true

	if err := toml.Unmarshal(CfgFile, gConfig); err != nil {
		panic(err)
	}

	util.MustPanic(Validate.Struct(gConfig))

	if gConfig.Broker.MaxQos > 2 || gConfig.Broker.MaxQos < 1 {
		gConfig.Broker.MaxQos = 2
	}

	fmt.Println(gConfig.String())
}

type GConfig struct {
	Version string `toml:"version"`
	Broker  `toml:"broker"`
	Connect `toml:"connect"`
	Auth    `toml:"auth"`
	Server  `toml:"server"`
	PProf   `toml:"pprof"`
	Log     `toml:"log"`
}

func (cfg *GConfig) String() string {
	b, err := json.Marshal(*cfg)
	if err != nil {
		return fmt.Sprintf("%+v", *cfg)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "    ")
	if err != nil {
		return fmt.Sprintf("%+v", *cfg)
	}
	return out.String()
}

type Connect struct {
	Keepalive      int   `toml:"keepalive"  validate:"default=100"`
	ReadTimeout    int   `toml:"readTimeout"  validate:"default=3"`
	WriteTimeout   int   `toml:"writeTimeout"  validate:"default=3"`
	ConnectTimeout int   `toml:"connectTimeout" validate:"default=1000"`
	AckTimeout     int   `toml:"ackTimeout" validate:"default=5000"`
	TimeoutRetries int   `toml:"timeOutRetries" validate:"default=2"`
	Quota          int64 `toml:"quota" validate:"default=0"`
	QuotaLimit     int   `toml:"quotaLimit" validate:"default=0"`
}

type Auth struct {
	Allows []string `toml:"allows"`
}

type Server struct {
	Redirects         []string `tome:"redirects"`
	RedirectOpen      bool     `tome:"redirectOpen"`
	RedirectIsForEver bool     `tome:"redirectIsForEver"`
}

func setIntOrUint(fl validator.FieldLevel) bool {
	va, err := strconv.ParseInt(fl.Param(), 10, 64)
	if err != nil {
		return false
	}

	switch fl.Field().Kind() {
	case reflect.Int:
		fl.Field().Set(reflect.ValueOf(int(va)))
	case reflect.Uint:
		fl.Field().Set(reflect.ValueOf(uint(va)))
	case reflect.Int8:
		fl.Field().Set(reflect.ValueOf(int8(va)))
	case reflect.Uint8:
		fl.Field().Set(reflect.ValueOf(uint8(va)))
	case reflect.Int16:
		fl.Field().Set(reflect.ValueOf(int16(va)))
	case reflect.Uint16:
		fl.Field().Set(reflect.ValueOf(uint16(va)))
	case reflect.Int32:
		fl.Field().Set(reflect.ValueOf(int32(va)))
	case reflect.Uint32:
		fl.Field().Set(reflect.ValueOf(uint32(va)))
	case reflect.Int64:
		fl.Field().Set(reflect.ValueOf(int64(va)))
	case reflect.Uint64:
		fl.Field().Set(reflect.ValueOf(uint64(va)))
	}
	return true
}

func setFloat(fl validator.FieldLevel) bool {
	va, err := strconv.ParseFloat(fl.Param(), 64)
	if err != nil {
		return false
	}

	switch fl.Field().Kind() {
	case reflect.Float32:
		fl.Field().Set(reflect.ValueOf(float32(va)))
	case reflect.Float64:
		fl.Field().Set(reflect.ValueOf(float64(va)))
	}
	return true
}

func Translate(errs error) error {
	if errs == nil {
		return errs
	}
	if err, ok := errs.(validator.ValidationErrors); ok {
		var errList []string
		for _, e := range err {
			// can translate each error one at a time.
			errList = append(errList, e.Translate(trans))
		}
		return errors.New(strings.Join(errList, "|"))
	} else {
		return errs
	}
}
