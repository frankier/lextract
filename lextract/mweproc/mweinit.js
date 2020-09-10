conf.defaultTable = "joined";

conf.colOverrides = {
    "joined.ud_mwe_typ": {
        type: "enum",
        enumChoices: ["inflection", "multiword", "frame"]
    }
}