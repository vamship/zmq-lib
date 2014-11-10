# NodeJS Library Template

This is a sample application template, for [Node JS](http://nodejs.org) libraries. This template uses the following technologies:

###Application Tools
* [Node JS](http://nodejs.org/): Core platform; engine on which all server side code executes
* [Mocha JS](http://mochajs.org/): Testing framework for unit testing.
* [Winston](https://github.com/flatiron/winston): Logging framework.

###Development Tools
* [npm](https://www.npmjs.org/): Server side package manager. Manages server side dependencies (node modules).
* [grunt](http://gruntjs.com/): Javascript task automater. Used to automate common development tasks - builds, tests, etc.

###Project Structure
The project has the following structure.

```
<ROOT>
 |--- lib               [Source files for the library]
 |
 |--- test              [Test files]
 |   |--- unit          [Unit test files]
 |
 |--- logs              [Directory for log files]
 |
 |--- Gruntfile.js      [Grunt configuration file - provides basic tasks]
 |--- package.json      [Package configuration file - basic app info; node dependencies]
 |--- .jshintrc         [Configuration file containing JavaScript linting settings]
 |--- .gitignore        [List of files to be ignored by git]

```
The application and test code is completely self contained within the `lib` and `test` directories respectively.

##Usage

###Cloning
This template may be used by cloning the repository and subsequently pointing the cloned version to an upstream remote repository.

Given that this is a starter template, it is optimal if the repository is cloned with no history by executing
```
git clone <url> --depth 0
```
Once the template has been cloned, the upstream repo that this project points to may be changed by executing
```
git remote remove origin
git remote add origin <url>
```
Alternately, the developer can choose to delete the `.git` directory, and reinitialize git by executing:
```
rm -rf .git
git init
```

###Gruntfile
This project template is provided with a Gruntfile that contains task definitions for most common development activities. This includes - linting, testing and bumping version numbers. It is strongly recommended that the tasks defined in the Gruntfile be leveraged to the maximum extent possible. More information can be obtained by typing:
```
grunt help
```
