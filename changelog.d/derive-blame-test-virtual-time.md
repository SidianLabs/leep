### The busy-file blame test no longer depends on the machine's speed
- **Tests.** `first_blame_of_a_busy_file_is_not_one_read_per_change` measures
  the delays along the longest chain of reads on a paused clock instead of
  wall-clock time, so a loaded CI machine can't fail it. It keeps the same
  bounds on read count and waiting time.
