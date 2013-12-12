JAR := dist/pyleus-base.jar
SRC_FILES = $(shell find src/ -type f)

.PHONY: all
all: $(JAR)

$(JAR): pom.xml $(SRC_FILES)
	mvn package

.PHONY: clean
clean:
	rm -rf $(JAR) dist/ target/
