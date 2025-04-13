rootProject.name = "MangoDB"
include("src:test:integration")
findProject(":src:test:integration")?.name = "integration"
include("integration-testse")
