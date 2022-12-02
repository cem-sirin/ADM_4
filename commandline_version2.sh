#!/bin/sh
awk -F, 'BEGIN{FS=","} {if(NR>1){arr[$5]++}} END{asort(arr, sortedarr);for(l in arr){if(arr[l]==sortedarr[length(sortedarr)])print "location has the maximum number of purchases",l, arr[l]}}' bank_transactions.csv

awk -F, 'BEGIN{FS=","} {if(NR>1){arr[$4] = arr[$4]+$9}} END{asort(arr, sortedarr);for(l in arr){if(arr[l]==sortedarr[length(sortedarr)])print "gender that spent more",l, arr[l]}}' bank_transactions.csv

awk -F, 'BEGIN{FS=","} {if(NR>1){arr[$2]= arr[$2]+$9; count[$2]++}} END{for(c in arr){arr[c]= arr[c]/count[c]} asort(arr, sortedarr);for(l in arr){if(arr[l]==sortedarr[length(sortedarr)])print "customer with the highest average",l, arr[l]}}' bank_transactions.csv