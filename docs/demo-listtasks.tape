Output "docs/demo-listtasks.gif"
Set FontSize 16
Set Width 760
Set Height 260
Set Theme "Dracula"
Set WindowBar Colorful
Set WindowBarSize 36

Type "export PS1='$ '"
Enter
Sleep 300ms

Type "swarm-rd-cli --db demo2.db append task-2 agent-c 'spike validated against Ray actor object store' --kind result"
Enter
Sleep 800ms

Type "swarm-rd-cli --db demo2.db append task-3 agent-d 'separate task, confirms per-task isolation' --kind note"
Enter
Sleep 800ms

Type "swarm-rd-cli --db demo2.db list-tasks"
Enter
Sleep 1s

Type "swarm-rd-cli --db demo2.db --json pull task-2"
Enter
Sleep 2s
