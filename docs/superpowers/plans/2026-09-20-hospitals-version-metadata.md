# Hospitals version metadata recovery

1. Reproduce the missing STAC version note with a focused publisher test using a source quality manifest containing the Niyam IT attribution. Assert the generated catalog record retains the description and bounds.
2. Copy only authored version metadata from the source quality manifest into the published catalog record. Run the focused test and the datasets test suite.
3. Add an IaC regression assertion for the canonical published-storage slug, change the Dagster production setting, and run the IaC test suite.
4. Publish and deploy the two changes through the existing GitHub/IaC workflows, then run only the Hospitals v1.1.0 catalog publication. Check the STAC response and the live page’s version selector and attribution.
