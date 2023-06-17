use bitcask::{KvStore, KvsError};
use clap::{arg, Command};
use std::env::current_dir;
use std::process::exit;

fn main() -> Result<(), KvsError> {
    let matches = Command::new("bitcask")
        .version("1.0")
        .author("Vivi W. <polarsatellitest@gmail.com>")
        .about("Simple key-value data store")
        .subcommand(
            Command::new("set")
                .about("Set the value of a string key to a string")
                .arg(arg!(<KEY> "A string key"))
                .arg(arg!(<VALUE> "The string value of the key")),
        )
        .subcommand(
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(arg!(<KEY> "A string key")),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a given key")
                .arg(arg!(<KEY> "A string key")),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").unwrap();
            let value = sub_matches.get_one::<String>("VALUE").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_string(), value.to_string())?;
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_string()) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
