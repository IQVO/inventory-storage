package location

// TypeAutomatedStorage is facility-layout's well-known LocationType of an
// AS/RS logical location (facility-layout ADR 0048): one location per AS/RS,
// role Storage. Rack positions stay inside asrs-control.
const TypeAutomatedStorage = "AutomatedStorage"

// HoldsContainersOnly reports whether a bin of facility-layout LocationType
// locationType may hold only container stock (a tote or a sealed case, ADR
// 0054): the WCS retrieves containers, so loose eaches there could never be
// picked. An unknown type ("") does not, which is today's behaviour.
func HoldsContainersOnly(locationType string) bool { return locationType == TypeAutomatedStorage }
