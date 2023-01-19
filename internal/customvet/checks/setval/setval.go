package setval

import (
	"go/ast"
	"go/token"
	"go/types"
	"golang.org/x/tools/go/analysis"
)

var Analyzer = &analysis.Analyzer{
	Name: "setval",
	Doc:  "find Cmder types that are missing a SetVal method",

	Run: func(pass *analysis.Pass) (interface{}, error) {
		cmderTypes := make(map[string]token.Pos)
		typesWithSetValMethod := make(map[string]bool)

		for _, file := range pass.Files {
			for _, decl := range file.Decls {
				funcName, receiverType := parseFuncDecl(decl, pass.TypesInfo)

				switch funcName {
				case "Result":
					cmderTypes[receiverType] = decl.Pos()
				case "SetVal":
					typesWithSetValMethod[receiverType] = true
				}
			}
		}

		for cmder, pos := range cmderTypes {
			if !typesWithSetValMethod[cmder] {
				pass.Reportf(pos, "%s is missing a SetVal method", cmder)
			}
		}

		return nil, nil
	},
}

func parseFuncDecl(decl ast.Decl, typesInfo *types.Info) (funcName, receiverType string) {
	funcDecl, ok := decl.(*ast.FuncDecl)
	if !ok {
		return "", "" // Not a function declaration.
	}

	if funcDecl.Recv == nil {
		return "", "" // Not a method.
	}

	if len(funcDecl.Recv.List) != 1 {
		return "", "" // Unexpected number of receiver arguments. (Can this happen?)
	}

	receiverTypeObj := typesInfo.TypeOf(funcDecl.Recv.List[0].Type)
	if receiverTypeObj == nil {
		return "", "" // Unable to determine the receiver type.
	}

	return funcDecl.Name.Name, receiverTypeObj.String()
}
