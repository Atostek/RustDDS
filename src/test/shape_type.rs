use serde::{Deserialize, Serialize};

use crate::dds::traits::key::Keyed;

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ShapeType {
  a: i32,
}

impl Keyed for ShapeType {
  type K = i32;
  fn key(&self) -> Self::K {
    self.a
  }
}
