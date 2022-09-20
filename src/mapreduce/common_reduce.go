package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
type ByOrder []keyValue

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) { // TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).

	// Remember that you've enc values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by croded theeating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
	intermediateFilesToProcess := createArrayIntermediateFileToProcess(jobName, reduceTaskNumber, nMap)
	proccessIntermediateFile(intermediateFilesToProcess)
}

func createArrayIntermediateFileToProcess(jobName string, numberReducedTasks int, numberMapTasks int) []string {
	index := 0
	intermediateFilesToProcess := []string{}
	for index < numberMapTasks {
		fileName := reduceName(jobName, index, numberReducedTasks)
		intermediateFilesToProcess = append(intermediateFilesToProcess, fileName)
		index++
	}
	return intermediateFilesToProcess
}
func proccessIntermediateFile(intermediateFilesToProcess []string) {
	bufferSortedKeyValuePairs := []KeyValue{}
	for _, fileName := range intermediateFilesToProcess {
		keyValuePair := readFileContent(fileName)
		sortedKeyValuePair := sortByKey(keyValuePair)
		bufferSortedKeyValuePairs = updateBufferedKeyValuePairNewSet(bufferSortedKeyValuePairs, sortedKeyValuePair)
	}
}
func updateBufferedKeyValuePairNewSet(bufferSortedKeyValuePairs, sortedKeyValuePair []KeyValue) []KeyValue {
	for _, keyPair := range sortedKeyValuePair {
		bufferSortedKeyValuePairs = append(bufferSortedKeyValuePairs, keyPair)
	}
	return bufferSortedKeyValuePairs
}
func sortByKey(keyValuePair []KeyValue) []KeyValue {
	sort.Slice(keyValuePair, func(i, j int) bool {
		return keyValuePair[i].Key < keyValuePair[j].Key
	})
	return keyValuePair
}

func readFileContent(fileName string) []KeyValue {
	content, err := os.ReadFile(fileName)
	var keyValuePairs []KeyValue
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(content, &keyValuePairs)
	if err != nil {
		log.Fatal(err)
	}
	return keyValuePairs
}
