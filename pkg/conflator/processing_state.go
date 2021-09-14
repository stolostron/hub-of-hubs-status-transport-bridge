package conflator

type conflationUnitProcessingState string

const (
	none         conflationUnitProcessingState = "none"
	inReadyQueue conflationUnitProcessingState = "inReadyQueue"
	inProcess    conflationUnitProcessingState = "inProcess"
)
