# find results with exceptions and create traces for these tx pairs
grep -lR 'exception' out/results/ | sed -E 's/^.*(0x\w+)_(0x\w+).*$/\1,\2/' > /tmp/hashes.csv; t_race trace --transactions-csv /tmp/hashes.csv
