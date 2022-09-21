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

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	intermediateFilesToProcess := createArrayIntermediateFileNameToProcess(jobName, reduceTaskNumber, nMap)
	processedWithReducedFunc := proccessIntermediateFile(intermediateFilesToProcess, reduceF)
	mergeFile := mergeName(jobName, reduceTaskNumber)
	addMapFileKeyRightFile(mergeFile, processedWithReducedFunc)
}

func createArrayIntermediateFileNameToProcess(jobName string, numberReducedTasks int, numberMapTasks int) []string {
	index := 0
	intermediateFilesToProcess := []string{}
	for index < numberMapTasks {
		fileName := reduceName(jobName, index, numberReducedTasks)
		intermediateFilesToProcess = append(intermediateFilesToProcess, fileName)
		index++
	}
	return intermediateFilesToProcess
}
func proccessIntermediateFile(intermediateFilesToProcess []string, reduceF func(key string, values []string) string) []KeyValue {
	bufferSortedKeyValuePairs := []KeyValue{}
	for _, fileName := range intermediateFilesToProcess {
		keyValuePair := readFileContent(fileName)
		sortedKeyValuePair := sortByKey(keyValuePair)
		bufferSortedKeyValuePairs = append(bufferSortedKeyValuePairs, sortedKeyValuePair...)
	}
	processedWithReducedFunc := runReducedFuncKeyValuePairs(bufferSortedKeyValuePairs, reduceF)
	return processedWithReducedFunc
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

func sortByKey(keyValuePair []KeyValue) []KeyValue {
	sort.Slice(keyValuePair, func(i, j int) bool {
		return keyValuePair[i].Key < keyValuePair[j].Key
	})
	return keyValuePair
}

func runReducedFuncKeyValuePairs(bufferSortedKeyValuePairs []KeyValue, reduceF func(key string, values []string) string) []KeyValue {
	keyValuePairsProcessed := []KeyValue{}
	emptyString := []string{}
	for _, keyPair := range bufferSortedKeyValuePairs {
		keyPair.Value = reduceF(keyPair.Key, emptyString)
		keyValuePairsProcessed = append(keyValuePairsProcessed, keyPair)
	}
	return keyValuePairsProcessed
}
