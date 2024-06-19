# count the instructions that cause TOD
grep -zRoP '"found": true(.*\n){8}' out/results | grep --text opcode | grep --text -oP '\d+$' | sort | uniq -c
