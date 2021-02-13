# `thesis-disk-minder`

This tool was written to make sure a machine didn't run out of storage when I
was running the experiments for my master thesis. It will do this by checking
a set of kafka topics for updates, and when it determines that the benchmark
is complete, it will print out the difference between the last and first messages
timestamp into a file, then delete the topic.

This was needed when I upped the amount of data by 10x and I was running on
an azure machine with only 30GiB of disk storage.

Interesting notes about this setup is that it replaces a much more complex program
written by the original paper authors, but it is much faster and does much more.
It also does not have any external dependencies.