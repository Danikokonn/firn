# ❄️ Firn - Go Bindings for Polars

Оригинальный README.md в части инструкций по сборке и документирования возможностей вводит в заблуждение, но в части описания архитектуры проекта он полезен для понимания, в данном форке он вынесен в README,ORIGINAL.md
В README.md этого форка описан рабочий способ сборки и использования модуля в проекте go. Также кратко описана схема добавления функционала в библиотеку.

### Тестировал сборку на wsl ubuntu 24.04

Сборка по документации у меня не сработала, так же как и многие функции из документации.

#### Шаги для подготовки среды и сборки
1. ```wget https://github.com/bazelbuild/bazel/releases/download/7.7.1/bazel-7.7.1-installer-linux-x86_64.sh```
2. ```sudo chmod +x ./bazel-7.7.1-installer-linux-x86_64.sh```
3. ```./bazel-7.7.1-installer-linux-x86_64.sh --user```
4. ```export PATH="$PATH:$HOME/bin"```
5. ```bazel version```
6. ```rustup install nightly```
7. ```rustup default nightly```
8. ```rustup target add x86_64-unknown-linux-gnu```
9. ```rustup target add aarch64-unknown-linux-gnu```
10. В `.bazelrc` 16 строку заменить на:
`build:linux --platforms=@rules_go//go/toolchain:linux_amd64`
11. Cоздать папку `lib` в корне 
12. Собрать rust ```./scripts/build_rust.sh```
13. добавить флаги для компоновщика в `dataframe_linux_amd64.go` 7 строка:
```#cgo LDFLAGS: -L${SRCDIR}/../lib -l:libfirn_linux_amd64.a -lm -lpthread -ldl```
14. Запустить тесты ```go test ./polars -v```
15. Удалить файл `dataframe_darwin_arm64.go` - компилировали для линукса, мак не нужен
15. Теперь можно взять папки polars и lib и переместить в свой проект и импортировать как:
```import "foo/polars"```

#### Примечание
При пересборке чистить rust в папке rust/:
```
cargo clean
rm -rf target/
```

и чистить go в папке вашего проекта:
```
go clean -cache
go clean -modcache
```

### Добавление фич:
1. Добавить функцию в проект rust, например, в rust/src/dataframe.rs
2. Пересобрать проект rust 
3. Записать экспортируемую сигнатуру функции в polars/firn.h
4. Написать функцию go, которая будет вызывать внешнюю функцию в модуле polars

При добавлении фич будет нужно обращаться к документации polars для rust:
https://docs.rs/polars/latest/polars/ - руководство
https://docs.rs/polars/latest/polars/all.html - api ref

Проверять фичи можно через файл sandbox.go