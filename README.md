# DEPRECATION NOTICE

All plugin development has moved to https://github.com/RADAR-base/radar-commons-android in the plugins directory. Please view that directory for examples of a plugin.

# Biovotion RADAR-pRMT plugin

Biovotion VSM plugin for the RADAR-AndroidApplication app, to be run on an Android 4.4 (or later) device with Bluetooth Low Energy (Bluetooth 4.0 or later), to interact with Biovotion VSM devices.

## Installation

First, add the plugin code to your application:

```gradle
repositories {
    flatDir { dirs 'libs' }
    maven { url  'http://dl.bintray.com/radar-cns/org.radarcns' }
}

dependencies {
    compile 'org.radarcns:radar-android-biovotion:0.1-alpha.1'
}
```

Also, copy the two Biovotion `.aar` files to your 'app/libs' directory.

## Contributing

To build this project, add the two Biovotion `.aar` files to the `libs/` directory and open it with Android Studio.

Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html), except using 4 spaces as indentation. Make a pull request once the code is working.
