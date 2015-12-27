zkmap
-----

A Go map backed by zookeeper.

Usage
-----

```
m := zkmap.New("127.0.0.1:2181", "/instance")
m.Set("key", "value")
fmt.Println(m.Get("key"))
```

License
-------
MIT
