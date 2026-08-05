Output docs/demo.gif

Set Shell "bash"
Set FontSize 18
Set Width 1100
Set Height 600
Set Padding 20
Set Theme "Dracula"

Type "swarm-rd-cli --db demo.db append task-1 agent-a 'found a race condition in the retry loop' --kind result"
Enter
Sleep 800ms

Type "swarm-rd-cli --db demo.db append task-1 agent-b 'confirmed: retry loop is not holding the lock' --kind result"
Enter
Sleep 800ms

Type "swarm-rd-cli --db demo.db pull task-1"
Enter
Sleep 2s
