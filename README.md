# nu_plugin_nuts
Nushell plugin for communicating with a Nats server

## Why
Nats already has a CLI tool and this plugin is not intended to replace it.
However, the official CLI tool's performance is not great when working with bulk data to a point that I would say
it's basically unusable. Publishing 1000 message of random bytes results in the following:

```bash
timeit { generate {|data| {out: $data, next: (random chars --length 8) } } (random chars --length 8) | take 1_000 | each { nats pub mysubject $in e> /dev/null } }
1min 1sec 656ms 305µs 542ns 
```

Even with `par-each`, while better, the situation is not that great:

```bash
timeit { generate {|data| {out: $data, next: (random chars --length 8) } } (random chars --length 8) | take 1_000 | par-each { nats pub mysubject $in e> /dev/null } }
36sec 502ms 574µs 792ns
```

With this plugin the performance of bulk operations are much better even on a cold start:

```bash
timeit { generate {|data| {out: $data, next: (random chars --length 8) } } (random chars --length 8) | take 1_000 | nuts connect | nuts pub mysubject }
61ms 217µs 833ns
```

While the Nats CLI has a bulk publish operation it relies on Go templating which is not always preferable.
It also performs worse than this plugin:

```bash
timeit { ^nats pub --count=1000 mysubject "{{ Random 8 8 }}" }
703ms 391µs 125ns
```

## Installing
Since Nushell is still in pre-production stage the recommended way of installing is to clone the repository,
change the nu protocol version to your Nushell version and build it yourself.

```bash
git clone https://github.com/dam4rus/nu_plugin_nuts.git
cd nu_plugin_nuts
cargo install --path .
plugin add ~/.cargo/bin/nu_plugin_nuts
plugin use nuts
```

## Usage
The plugin currently stores a single connection to a Nats server.
This connection can be opened with the `nuts connect` command. This connection is open for the lifetime of the plugin.

One way to use this plugin is to call `nuts connect` then call other operations while the plugin is alive.

```bash
nuts connect
'message' | nuts pub mysubject
```

Make sure to set the gc for the plugin to a duration you are comfortable with.

`nuts connect` also pipes it's input to it's output, so it can be used in a pipe operation as well:

```bash
'message' | nuts connect | nuts pub mysubject
```

Refer to each commands documentation to see it's usage
