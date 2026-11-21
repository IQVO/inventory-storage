Feature: Totes the WCS moves (ADR 0054)
  The WMS owns the container: its id, its contents and its logical location.
  The AS/RS is one logical location; rack positions never reach the ledger.

  Background:
    Given bin "WH1-RCV-DEC-D01-01-01-A" of site "WH1" with capacity 100
    And bin "WH1-AUTO-ASRS-ASRS1-00-00-S" of site "WH1" with capacity 100000 of location type "AutomatedStorage"
    And bin "WH1-STOR-RSV-A01-01-01-A" of site "WH1" with capacity 100 of location type "PalletRack"
    And tote "TOTE-1" is registered

  Scenario: Putaway - after the stow, the tote's contents are in the AS/RS location
    Given tote "TOTE-1" is stowed into "WH1-RCV-DEC-D01-01-01-A"
    When 6 of "SKU-1" and 4 of "SKU-2" are filled into tote "TOTE-1"
    And tote "TOTE-1" is stowed into "WH1-AUTO-ASRS-ASRS1-00-00-S"
    Then tote "TOTE-1" is in "WH1-AUTO-ASRS-ASRS1-00-00-S" holding 6 of "SKU-1" and 4 of "SKU-2"
    And bin "WH1-AUTO-ASRS-ASRS1-00-00-S" holds 10 eaches and bin "WH1-RCV-DEC-D01-01-01-A" holds 0
    And the events "ToteRegistered, ToteStowed, ToteFilled, ToteFilled, ToteStowed" were raised for "TOTE-1" in that order

  Scenario: Goods-to-person - allocation from automated storage names one container
    Given tote "TOTE-1" in "WH1-AUTO-ASRS-ASRS1-00-00-S" holds 3 of "SKU-1"
    And tote "TOTE-2" in "WH1-AUTO-ASRS-ASRS1-00-00-S" holds 10 of "SKU-1"
    When 8 of "SKU-1" are reserved for "order-42"
    Then the reservation has one allocation of 8 from container "TOTE-2" in "WH1-AUTO-ASRS-ASRS1-00-00-S"

  Scenario: A goods-to-person trip never moves stock logically
    Given tote "TOTE-2" in "WH1-AUTO-ASRS-ASRS1-00-00-S" holds 10 of "SKU-1"
    And 8 of "SKU-1" are reserved for "order-42"
    When tote "TOTE-2" is stowed into "WH1-RCV-DEC-D01-01-01-A"
    Then the stow is refused with "tote-reserved"
    And tote "TOTE-2" is in "WH1-AUTO-ASRS-ASRS1-00-00-S" holding 10 of "SKU-1"

  Scenario: Loose stock never enters the AS/RS location
    When 5 of "SKU-1" are stowed loose into "WH1-AUTO-ASRS-ASRS1-00-00-S"
    Then the stow is refused with "automated-storage-requires-container"
    And bin "WH1-AUTO-ASRS-ASRS1-00-00-S" holds 0 eaches and bin "WH1-RCV-DEC-D01-01-01-A" holds 0

  Scenario: One license plate, one container
    Given a sealed case "003400000000000017" of 12 "SKU-1" was received
    When tote "003400000000000017" is registered
    Then the registration is refused with "license-plate-taken"
