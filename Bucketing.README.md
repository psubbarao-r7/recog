**this is just a draft, README is in progress, more notes forthcoming**
**before you feed it in for templatization, insert a space before and after all numbers using awk or sed**
	e.g.:
		cat filename.txt | awk '{gsub(/[0-9]+/," & ",$0); $1=$1}1'
Then process templatization.
Recommended parameters: depth=3, st=0.9
