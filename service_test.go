package cache

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fighterlyt/gormlogger"
	"github.com/fighterlyt/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	dsn         = `root:dubaihell@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=True&loc=Local`
	db          *gorm.DB
	err         error
	testLogger  log.Logger
	testService Manager
	testClient  Client
	result      interface{}
	typ         Type
	load        Load
)

func TestMain(m *testing.M) {
	if testLogger, err = log.NewEasyLogger(true, false, ``, `缓存`); err != nil {
		panic(`构建日志器` + err.Error())
	}

	if db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormlogger.NewLogger(testLogger.SetLevel(zapcore.DebugLevel), time.Second, nil),
	}); err != nil {
		panic(`gorm` + err.Error())
	}

	os.Exit(m.Run())
}

func TestNewService(t *testing.T) {
	newService(t)
}

func newService(tb testing.TB) {
	testService, err = NewService(testLogger, `localhost:9736`, `dubaihell`, 9)
	require.NoError(tb, err)
}

func TestManager_Register(t *testing.T) {
	TestNewService(t)

	key := `1`

	var (
		origin interface{}
		count  = 10000
	)

	load, err = DBLoad(db.Model(&Currency{}), `id`, ``, `currency`, Int64Convert, func() interface{} {
		return &Currency{}
	})

	require.NoError(t, err)

	origin, err = load(context.Background(), key)
	require.NoError(t, err, `加载`)

	typ, err = NewTypeTmpl(`currency`, load, func() interface{} {
		return &Currency{}
	})

	require.NoError(t, err)

	testClient, err = testService.Register(typ, time.Minute, RedisAndMem)

	require.NoError(t, err)

	for i := 0; i < count; i++ {
		result, err = testClient.Get(`1`)
		require.NoError(t, err)

		require.EqualValues(t, origin, result)
	}
	t.Log(origin)
}

func BenchmarkManager_Register(b *testing.B) { //nolint:golint,revive
	newService(b)

	b.ReportAllocs()

	countLoad = true

	load, err = DBLoad(db.Model(&Currency{}), `id`, ``, `currency`, Int64Convert, func() interface{} {
		return &Currency{}
	})

	typ, err = NewTypeTmpl(`currency`, load, func() interface{} {
		return &Currency{}
	})

	require.NoError(b, err)

	testClient, err = testService.Register(typ, time.Minute, RedisAndMem)

	require.NoError(b, err)

	concurrent := 10

	b.Run(`使用缓存`, func(b *testing.B) {
		wg := &sync.WaitGroup{}
		wg.Add(concurrent)
		loadCount.Store(0)
		for j := 0; j < concurrent; j++ {
			go func() {
				for i := 0; i < b.N; i++ {
					result, err = testClient.Get(`1`)
					require.NoError(b, err)
				}
				wg.Done()
			}()
		}
		wg.Wait()

		b.ReportMetric(float64(loadCount.Load()), `load次数`)
	})

	b.Run(`不使用缓存`, func(b *testing.B) {
		wg := &sync.WaitGroup{}
		wg.Add(concurrent)
		loadCount.Store(0)

		for j := 0; j < concurrent; j++ {
			go func() {
				for i := 0; i < b.N; i++ {
					result, err = testClient.Get(`1`)
					require.NoError(b, err)

					require.NoError(b, testClient.Invalidate(`1`))
				}
				wg.Done()
			}()
		}

		wg.Wait()
		b.ReportMetric(float64(loadCount.Load()), `load次数`)
	})
}

// Currency 币种信息
type Currency struct {
	ID         int64  `gorm:"primary_key;column:id;type:bigint(20) unsigned AUTO_INCREMENT;not null;comment:'ID'"` // ID
	IMG        int64  `gorm:"column:img;type:bigint(11);comment:币种图标"`                                             // 币种图标
	Name       string `gorm:"column:name;type:varchar(255);uniqueIndex;comment:平台币种名称"`                            // 平台币种名称
	AliasName  string `gorm:"column:alias_name;type:varchar(255);comment:平台币种别名"`                                  // 平台币种别名
	IsTrue     int    `gorm:"column:is_true;type:bigint(11);comment:是否真实创建用户地址"`                                   // 是否真实创建用户地址
	CreateTime int64  `gorm:"column:create_time;type:bigint(11);comment:创建时间"`                                     // 创建时间
	UpdateTime int64  `gorm:"column:update_time;type:bigint(11);comment:更新时间"`                                     // 更新时间
	Sort       int64  `gorm:"column:sort;type:bigint;comment:展示顺序"`                                                // 展示顺序
}

func (c *Currency) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c Currency) MarshalBinary() (data []byte, err error) {
	return json.Marshal(c)
}

func (c Currency) TableName() string {
	return `platform_currency`
}

func TestManager_RegisterByName(t *testing.T) {
	TestNewService(t)

	prefix := `currencyByName`

	load, err = DBLoad(db.Model(&Currency{}), `name`, ``, prefix, NoopConvert, func() interface{} {
		return &Currency{}
	})

	require.NoError(t, err)

	typ, err = NewTypeTmpl(prefix, load, func() interface{} {
		return &Currency{}
	})

	require.NoError(t, err)

	testClient, err = testService.Register(typ, time.Minute, OnlyRedis)

	require.NoError(t, err)

	result, err = testClient.Get(`ACW`)
	require.NoError(t, err)
	t.Log(result)

	time.Sleep(time.Second)
}

func TestManager_RegisterByIDBatch(t *testing.T) {
	TestNewService(t)

	prefix := `currencyByIDBatch`

	convert := func(s string) (interface{}, error) {
		fields := strings.Split(s, `-`)

		var (
			start, end int64
			ids        []int64
		)

		start, _ = strconv.ParseInt(fields[0], 10, 64)
		end, _ = strconv.ParseInt(fields[1], 10, 64)

		for i := start; i <= end; i++ {
			ids = append(ids, i)
		}

		return ids, nil
	}

	load, err = DBLoad(db.Model(&Currency{}), `id`, `in`, prefix, convert, func() interface{} {
		return &currencies{}
	})

	require.NoError(t, err)

	typ, err = NewTypeTmpl(prefix, load, func() interface{} {
		return &currencies{}
	})

	require.NoError(t, err)

	testClient, err = testService.Register(typ, time.Minute, OnlyRedis)

	require.NoError(t, err)

	result, err = testClient.Get(`1-10`)
	require.NoError(t, err)
	t.Log(result)

	time.Sleep(time.Second)
}

type currencies []Currency

func (c *currencies) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c currencies) MarshalBinary() (data []byte, err error) {
	return json.Marshal(c)
}
