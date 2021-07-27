package cache

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"gorm.io/gorm"
)

const (
	ten       = 10
	sixtyFour = 64
)

// Convert 转化方法,负责将对象的查询key转换为数据库对应字段的值
type Convert func(string) (interface{}, error)

type Load func(ctx context.Context, key interface{}) (interface{}, error)

var (
	// Int64Convert 转换为整数
	Int64Convert Convert = func(value string) (interface{}, error) {
		intValue, err := strconv.ParseInt(value, ten, sixtyFour)

		if err != nil {
			return nil, errors.Wrapf(err, `key值必须是整数[%s]`, value)
		}

		return intValue, nil
	}
	// NoopConvert 什么都不做
	NoopConvert Convert = func(s string) (interface{}, error) {
		return s, nil
	}
)

// typeTmpl Type 的实现
type typeTmpl struct {
	cachePrefix string
	load        func(ctx context.Context, key interface{}) (interface{}, error)
	newFunc     func() interface{}
}

/*NewTypeTmpl 新建一个Type实现
参数:
*	cachePrefix	string                  前缀
*	load       	Load	                加载方法
*	newFunc    	func() interface{}      新建方法
返回值:
*	result  	*typeTmpl
*   err         error
*/
func NewTypeTmpl(cachePrefix string, load Load, newFunc func() interface{}) (result *typeTmpl, err error) {
	if strings.TrimSpace(cachePrefix) == `` {
		return nil, errors.New(`前缀不能为空`)
	}

	if load == nil {
		return nil, errors.New(`加载方法不能为空`)
	}

	if newFunc == nil || newFunc() == nil || reflect.TypeOf(newFunc()).Kind() != reflect.Ptr {
		return nil, errors.New(`新建方法不能为空，且不能返回nil,且只能返回指针`)
	}

	return &typeTmpl{
		cachePrefix: cachePrefix,
		load:        load,
		newFunc:     newFunc,
	}, nil
}

func (r typeTmpl) CachePrefix() string {
	return r.cachePrefix
}
func (r typeTmpl) New() interface{} {
	return r.newFunc()
}

func (r typeTmpl) Load(ctx context.Context, key interface{}) (interface{}, error) {
	return r.load(ctx, key)
}

var (
	loadCount = atomic.NewInt64(0)
	countLoad = false
)

/*DBLoad 方法说明
参数:
*	db     	*gorm.DB          	数据库
*	field  	string            	数据库字段名
*	prefix 	string            	前缀
*	convert	Convert           	转换方法，不能为空
*	newFunc	func() interface{}	参数5
返回值:
*	load   	Load              	返回值1
*	err    	error             	返回值2
*/
func DBLoad(db *gorm.DB, field, operator, prefix string, convert Convert, newFunc func() interface{}) (load Load, err error) {
	if err = dbloadValidate(db, convert, newFunc); err != nil {
		return nil, errors.Wrap(err, `参数校验`)
	}

	if field == `` {
		field = `id`
	}
	if operator == `` {
		operator = `=`
	}

	return func(ctx context.Context, key interface{}) (interface{}, error) {
		str, ok := key.(string)

		if !ok {
			return nil, errors.New(`key 必须是字符串`)
		}

		str = strings.TrimPrefix(str, prefix+delimiter)

		value, err := convert(str)

		if err != nil {
			return nil, errors.Wrapf(err, `key值必须是整数[%s]`, str)
		}

		result := newFunc()

		if err := db.Debug().Where(fmt.Sprintf(`%s %s ?`, field, operator), value).Find(result).Error; err != nil {
			return nil, err
		}

		if countLoad {
			loadCount.Inc()
		}

		return result, nil
	}, nil
}

func dbloadValidate(db *gorm.DB, convert Convert, newFunc func() interface{}) error {
	if db == nil {
		return errors.New(`db不能为空`)
	}

	if convert == nil {
		return errors.New(`转换方法方不能为空`)
	}

	if newFunc == nil || newFunc() == nil || reflect.TypeOf(newFunc()).Kind() != reflect.Ptr {
		return errors.New(`新建方法不能为空，且不能返回nil,且只能返回指针`)
	}

	return nil
}
