JAR := dist/pyleus-base.jar
SRC_FILES = $(shell find src/ -type f)

all: $(JAR)

$(JAR): pom.xml $(SRC_FILES)
	mvn package

clean:
	rm -rf $(JAR) dist/ target/

.PHONY: all clean
