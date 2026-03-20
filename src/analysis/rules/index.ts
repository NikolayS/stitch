/**
 * Analysis rules registry.
 *
 * Each rule is imported from its own file and re-exported here.
 * The analysis engine (issue #50) will use this registry to discover
 * and invoke rules.
 */

export { SA001 } from "./SA001.js";
export { SA002 } from "./SA002.js";
export { SA002b } from "./SA002b.js";
export { SA003 } from "./SA003.js";
export { SA004 } from "./SA004.js";
export { SA005 } from "./SA005.js";
export { SA006 } from "./SA006.js";
export { SA007 } from "./SA007.js";
export { SA008 } from "./SA008.js";
export { SA009 } from "./SA009.js";
export { SA010 } from "./SA010.js";
export { SA011 } from "./SA011.js";
export { SA012 } from "./SA012.js";
export { SA013 } from "./SA013.js";
export { SA014 } from "./SA014.js";
export { SA015 } from "./SA015.js";
export { SA016 } from "./SA016.js";
export { SA017 } from "./SA017.js";
export { SA018 } from "./SA018.js";
export { SA019 } from "./SA019.js";
export { SA020 } from "./SA020.js";
export { SA021 } from "./SA021.js";

import { SA001 } from "./SA001.js";
import { SA002 } from "./SA002.js";
import { SA002b } from "./SA002b.js";
import { SA003 } from "./SA003.js";
import { SA004 } from "./SA004.js";
import { SA005 } from "./SA005.js";
import { SA006 } from "./SA006.js";
import { SA007 } from "./SA007.js";
import { SA008 } from "./SA008.js";
import { SA009 } from "./SA009.js";
import { SA010 } from "./SA010.js";
import { SA011 } from "./SA011.js";
import { SA012 } from "./SA012.js";
import { SA013 } from "./SA013.js";
import { SA014 } from "./SA014.js";
import { SA015 } from "./SA015.js";
import { SA016 } from "./SA016.js";
import { SA017 } from "./SA017.js";
import { SA018 } from "./SA018.js";
import { SA019 } from "./SA019.js";
import { SA020 } from "./SA020.js";
import { SA021 } from "./SA021.js";

import type { Rule } from "../types.js";

/** All registered analysis rules */
export const allRules: Rule[] = [
  SA001,
  SA002,
  SA002b,
  SA003,
  SA004,
  SA005,
  SA006,
  SA007,
  SA008,
  SA009,
  SA010,
  SA011,
  SA012,
  SA013,
  SA014,
  SA015,
  SA016,
  SA017,
  SA018,
  SA019,
  SA020,
  SA021,
];

/** Look up a rule by ID */
export function getRule(id: string): Rule | undefined {
  return allRules.find((r) => r.id === id);
}
