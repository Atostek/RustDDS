#[cfg(feature = "security")]
use anyhow::Result;
#[cfg(feature = "security")]
use cryptoki::{
  context::{CInitializeArgs, CInitializeFlags, Pkcs11},
  object::AttributeType,
  session::UserType,
  types::AuthPin,
};

#[cfg(not(feature = "security"))]
fn main() {
  println!("This example requires the cargo feature \"security\".");
}

#[cfg(feature = "security")]
fn main() -> Result<()> {
  // change here any PKCS#11 library implementation that you are using.
  let pkcs11client = Pkcs11::new("/usr/lib/softhsm/libsofthsm2.so")?;

  println!("Initializing Cryptoki.");
  pkcs11client.initialize(CInitializeArgs::new(CInitializeFlags::OS_LOCKING_OK))?;
  println!("Initialized.");

  println!("Library info: {:?}", pkcs11client.get_library_info());

  let interesting_attributes = vec![
    AttributeType::Class,
    AttributeType::Label,
    AttributeType::KeyType,
    AttributeType::Sign,
    AttributeType::Sensitive,
  ];

  let slots = pkcs11client.get_all_slots()?;
  println!("Found {} slots.", slots.len());
  for (num, slot) in slots.iter().enumerate() {
    let slot_info = pkcs11client.get_slot_info(*slot);
    println!("\n{num}:\n{slot_info:?}");
    match slot_info {
      Ok(si) if si.token_present() => {
        println!("token info: {:?}", pkcs11client.get_token_info(*slot));
        //println!("mechanisms: {:?}", pkcs11client.get_mechanism_list(*slot));
        match pkcs11client.open_ro_session(*slot) {
          Err(e) => println!("Session open failed: {e:?}"),
          Ok(session) => {
            println!("Session opened. Trying login.");
            let secret_pin = AuthPin::new("DDSTest_1234".into());
            let login_result = session.login(UserType::User, Some(&secret_pin));
            match login_result {
              Ok(()) => {
                println!("Login successful");
                match session.find_objects(&[]) {
                  Ok(object_handles) => {
                    for (k, obj) in object_handles.iter().enumerate() {
                      println!("  Object {k}");
                      match session.get_attributes(*obj, &interesting_attributes) {
                        Ok(attrs) => {
                          for a in attrs {
                            println!("    {a:?}");
                          }
                        }
                        Err(e) => println!("{e:?}"),
                      }
                    }
                  }
                  Err(e) => println!("find_objects error: {e:?}"),
                }
              }
              Err(e) => {
                println!("login failed: {e:?}");
              }
            }
          }
        }
      }
      Ok(_) => println!("No token."),
      Err(_) => {}
    }
  }
  Ok(())
}
