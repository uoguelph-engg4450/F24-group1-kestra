import JsYaml from "js-yaml";
import _cloneDeep from "lodash/cloneDeep"

export default class YamlUtils {
    static stringify(value) {
        if (typeof value === "undefined") {
            return "";
        }

        return JsYaml.dump(YamlUtils._transform(_cloneDeep(value)), {
            lineWidth: -1,
            noCompatMode: true,
            quotingType: "\"",
        });
    }

    static parse(item) {
        return JsYaml.load(item);
    }

    static _transform(value) {
        if (value instanceof Array) {
            return value.map(r => {
                return YamlUtils._transform(r);
            })
        } else if (typeof(value) === "string" || value instanceof String) {
            // value = value
            //     .replaceAll(/\u00A0/g, " ");
            //
            // if (value.indexOf("\\n") >= 0) {
            //     return value.replaceAll("\\n", "\n") + "\n";
            // }

            return value;
        } else if (value instanceof Object) {
           return Object.keys(value)
               .reduce((accumulator, r) => {
                   if (value[r] !== undefined) {
                       accumulator[r] = YamlUtils._transform(value[r])
                   }

                   return accumulator;
               }, Object.create({}))
        }

        return value;
    }
}
