# Review carries of WCS plan 05a pinned at the acceptance level (ADR 0054):
#   - carry (Task 6 review): a fill raises BinStockLevelChanged STOW for the
#     tote's bin, a stow RELOCATE for both bins (D12); a fill applies the
#     placement rules of a plain stow (hazmat refused, Placement wired).

Feature: Tote fills and stows, as a stow and a move (ADR 0054)
  A fill is a stow into the tote at its bin; a stow moves the tote and every
  unit of it. Both raise the bin level events a stow and a move raise.

  Background:
    Given bin "WH1-RCV-DEC-D01-01-01-A" of site "WH1" with capacity 100
    And bin "WH1-AUTO-ASRS-ASRS1-00-00-S" of site "WH1" with capacity 100000 of location type "AutomatedStorage"
    And tote "TOTE-1" is registered

  # carry (Task 6 review)
  Scenario: A fill raises a STOW level event and a stow a RELOCATE level event for both bins
    Given tote "TOTE-1" is stowed into "WH1-RCV-DEC-D01-01-01-A"
    When 6 of "SKU-1" are filled into tote "TOTE-1"
    And tote "TOTE-1" is stowed into "WH1-AUTO-ASRS-ASRS1-00-00-S"
    Then the BinStockLevelChanged events published are:
      | bin                         | onHand | reserved | reason   |
      | WH1-RCV-DEC-D01-01-01-A     | 6      | 0        | STOW     |
      | WH1-RCV-DEC-D01-01-01-A     | 0      | 0        | RELOCATE |
      | WH1-AUTO-ASRS-ASRS1-00-00-S | 6      | 0        | RELOCATE |

  # carry (Task 6 review)
  Scenario: A Hazmat SKU is refused a tote at a decant bin outside a hazmat-rated zone
    Given Bin "WH1-RCV-DEC-D01-01-01-A" sits in a non-hazmat "Ambient" zone
    And SKU "SKU-H" is classified by product-master as Hazmat with DOT hazard class 3
    And tote "TOTE-1" is stowed into "WH1-RCV-DEC-D01-01-01-A"
    When 2 of "SKU-H" are filled into tote "TOTE-1"
    Then the fill is refused with "hazmat-zone-required"
    And tote "TOTE-1" is in "WH1-RCV-DEC-D01-01-01-A" holding nothing
    And bin "WH1-RCV-DEC-D01-01-01-A" holds 0 eaches and bin "WH1-AUTO-ASRS-ASRS1-00-00-S" holds 0
