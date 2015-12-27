package zkmap

import (
	"fmt"
	"path"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type zkmap struct {
	zk     *zk.Conn
	root   string
	mutex  sync.RWMutex
	fields map[string]reflect.Value
}

// Create a map from given root path
func New(zkServer string, root string) (*zkmap, error) {
	c, _, err := zk.Connect([]string{zkServer}, time.Second)
	if err != nil {
		return nil, fmt.Errorf("can't connect to zookeeper %s: %v", zkServer, err)
	}

	exists, _, err := c.Exists(root)
	if err != nil {
		return nil, fmt.Errorf("can't check path %s: %v", root, err)
	}

	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	if !exists {
		_, err = c.Create(root, []byte(""), flags, acl)
		if err != nil {
			return nil, fmt.Errorf("can't create path %s: %v", root, err)
		}
	}

	m := &zkmap{zk: c, root: root, fields: make(map[string]reflect.Value)}
	return m, nil
}

// Set a value for given key
func (m *zkmap) Set(key string, value interface{}) error {
	t := reflect.ValueOf(value)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.fields[key] = t

	var data []byte
	switch t.Kind() {
	case reflect.Int, reflect.Int32:
		data = []byte(strconv.FormatInt(t.Int(), 10))
	case reflect.Bool:
		data = []byte(strconv.FormatBool(t.Bool()))
	case reflect.String:
		data = []byte(t.String())
	default:
		return fmt.Errorf("unsupported type %T value %v", value, value)
	}

	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	path := fmt.Sprintf("%s/%s", m.root, key)
	_, err := m.zk.Create(path, data, flags, acl)
	if err != nil {
		return fmt.Errorf("can't create path %s: %v", path, err)
	}
	return nil
}

func (m *zkmap) Get(key string) (interface{}, error) {

	path := fmt.Sprintf("%s/%s", m.root, key)
	exists, _, err := m.zk.Exists(path)
	if err != nil {
		return nil, fmt.Errorf("can't check path %s: %v", path, err)
	}
	if !exists {
		return nil, fmt.Errorf("path %s not existed: %v", path, err)
	}

	data, _, err := m.zk.Get(path)
	if err != nil {
		return nil, fmt.Errorf("can't get value from %s: %v", path, err)
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()
	t := m.fields[key]

	var value interface{}
	switch t.Kind() {
	case reflect.Int, reflect.Int32:
		value, _ = strconv.Atoi(string(data))
	case reflect.Bool:
		value, _ = strconv.ParseBool(string(data))
	case reflect.String:
		value = string(data)
	default:
		panic(fmt.Errorf("unsupported type %s", t.Kind()))
	}
	return value, nil
}

func deleteRecursive(conn *zk.Conn, zkPath string) error {
	err := conn.Delete(zkPath, -1)
	if err == nil {
		return nil
	}
	children, _, err := conn.Children(zkPath)
	for _, child := range children {
		err := deleteRecursive(conn, path.Join(zkPath, child))
		if err != nil {
			return fmt.Errorf("recursive delete failed: %v", err)
		}
	}
	err = conn.Delete(zkPath, -1)
	if err != nil {
		return fmt.Errorf("delete failed: %v", err)
	}
	return nil
}

func (m *zkmap) Delete() error {
	return deleteRecursive(m.zk, m.root)
}
