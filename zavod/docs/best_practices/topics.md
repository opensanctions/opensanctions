# Topics

Topics tag entities with the kind of risk or role they carry. The
[topics page](https://www.opensanctions.org/docs/topics/) defines what each one means,
and `ftm ref type topic` lists the full vocabulary.

## Risk topics while in force

Add a risk topic, such as `sanction`, `debarment` or `reg.action`, to a listed entity
only while its listing or measure is in force, that is while
[`h.is_active`][zavod.helpers.is_active] is true for its `Sanction`.

## Where topics go

Topics go on the listed entity. Linked parties get none (see
[sanctions](../sanctions/index.md#linked-parties)), and PEP positions get theirs as
described in [creating positions](../peps/index.md#creating-positions).
