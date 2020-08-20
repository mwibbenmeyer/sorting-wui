for file in *.dat
do 
  [[ $file =~ ([[:digit:]]+) ]] && mv "$file" "lar_$(printf "${BASH_REMATCH[0]}").dat"
done

for file in *.txt
do 
  [[ $file =~ ([[:digit:]]+) ]] && mv "$file" "lar_$(printf "${BASH_REMATCH[0]}").dat"
done

for file in *.DAT
do 
  [[ $file =~ ([[:digit:]]+) ]] && mv "$file" "lar_$(printf "${BASH_REMATCH[0]}").dat"
done

for file in *.csv
do 
  [[ $file =~ ([[:digit:]]+) ]] && mv "$file" "lar_$(printf "${BASH_REMATCH[0]}").dat"
done

for file in *.LARS
do 
  [[ $file =~ ([[:digit:]]+) ]] && mv "$file" "lar_$(printf "${BASH_REMATCH[0]}").dat"
done