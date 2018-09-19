direct:
	rm -f output.txt*
	mvn compile exec:java -Dexec.mainClass=com.mozilla.secops.HeavyHitters \
		-Dexec.args="--inputFile=input.txt --outputFile=output.txt" -Pdirect-runner
