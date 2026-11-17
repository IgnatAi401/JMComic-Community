# JMComic Community

[JMComic Next](https://github.com/IgnatAi401/JMComic-Next) 和 [JMComic Web](https://github.com/IgnatAi401/JMComic-Web) 共用的社区服务：

- **在线人数**：页面打开时每分钟发一次心跳，统计实时在线人数，并按小时记录平均在线、峰值在线和独立访客数。
- **社区推荐**：任何人都可以把一本漫画推荐到公共列表（不能撤销），并对列表里的作品投票：好看 / 不好看（二选一，可改），欢乐（单独勾选）。

全程匿名：浏览器只发送一个随机生成的 ID，服务端只保存它的哈希值；不记录 IP、不记录账号，访问日志也是关闭的。

只用 Python 标准库和 SQLite，没有第三方依赖。
