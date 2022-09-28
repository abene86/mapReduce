package mapreduce

import (
	"encoding/json"
	"io"
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
		bufferSortedKeyValuePairs = append(bufferSortedKeyValuePairs, keyValuePair...)
		bufferSortedKeyValuePairs = sortByKey(bufferSortedKeyValuePairs)
	}
	processedWithReducedFunc := runReducedFuncKeyValuePairs(bufferSortedKeyValuePairs, reduceF)
	return processedWithReducedFunc
}

func readFileContent(fileName string) []KeyValue {
	var keyPair KeyValue
	keyValuePairs := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	jsonDecoder := json.NewDecoder(file)
	errJson := jsonDecoder.Decode(&keyPair)
	for errJson != io.EOF {
		keyValuePairs = append(keyValuePairs, keyPair)
		errJson = jsonDecoder.Decode(&keyPair)
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
	valueStringKeys := getValue(bufferSortedKeyValuePairs)
	checkerAlreadyWent := createCheckerOfAlreadyWentForReducFunc(valueStringKeys)
	for _, keyPair := range bufferSortedKeyValuePairs {
		if checkerAlreadyWent[keyPair.Key] == 0 {
			keyPair.Value = reduceF(string(keyPair.Key), valueStringKeys)
			keyValuePairsProcessed = append(keyValuePairsProcessed, keyPair)
			checkerAlreadyWent[keyPair.Key] = 1
		}
		//fmt.Println("processedList", keyValuePairsProcessed)
	}
	return keyValuePairsProcessed
}
func createCheckerOfAlreadyWentForReducFunc(valueKey []string) map[string]int {
	checkerAlreadyWent := make(map[string]int)
	for _, key := range valueKey {
		checkerAlreadyWent[key] = 0
	}
	return checkerAlreadyWent
}

func getValue(bufferSortedKeyValuePairs []KeyValue) []string {
	valueStringKeys := []string{}
	for _, keyPair := range bufferSortedKeyValuePairs {
		valueStringKeys = append(valueStringKeys, string(keyPair.Key))
	}
	//fmt.Println("valueStringKeys", valueStringKeys)
	return valueStringKeys
}
