echo '1-The location that has the maximum number of purchases been made:'

awk -F, '{print($5)}' bank_transactions.csv | sort | uniq -c | sort -nr | head -1

echo '2-The most spending purchases are: '

awk -F, '{$4+=$9}END{ if(list["F"] > list["M"]){ print("F");} else{ print("M");}}' bank_transactions.csv

echo '3-The customer with the highest average transaction amount: '

awk -F, '{c[$2]++;list[$2]+=$9}END{for (i in list) print(list[i]/c[i],i)}' bank_transactions.csv | sort -nr | head -1


