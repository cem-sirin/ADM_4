echo '1-The location that has the maximum number of purchases been made:'

awk -F, '{print($5)}' data/bank_transactions.csv | sort | uniq -c | sort -nr | head -1

echo '2-The most spending purchases are: '

awk -F, '{$4+=$9}END{ if(list["F"] > list["M"]){ print("F");} else{ print("M");}}' data/bank_transactions.csv

echo '3-The customer with the highest average transaction amount: '

awk -F, 'BEGIN{FS=","} {if(NR>1){arr[$2]= arr[$2]+$9; count[$2]++}} END{for(c in arr){arr[c]= arr[c]/count[c]} asort(arr, sortedarr);for(l in arr){if(arr[l]==sortedarr[length(sortedarr)])print arr[l], l}}' bank_transactions.csv

