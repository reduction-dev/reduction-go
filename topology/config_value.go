package topology

import "reduction.dev/reduction-protocol/jobconfigpb"

/** String **/

type ResolvableString struct {
	value *string
	param *string
}

func (rs ResolvableString) Proto() *jobconfigpb.StringVar {
	if rs.value != nil {
		return &jobconfigpb.StringVar{
			Kind: &jobconfigpb.StringVar_Value{
				Value: *rs.value,
			},
		}
	} else if rs.param != nil {
		return &jobconfigpb.StringVar{
			Kind: &jobconfigpb.StringVar_Param{
				Param: *rs.param,
			},
		}
	}
	return nil
}

func StringValue(val string) ResolvableString {
	return ResolvableString{
		value: &val,
	}
}

func StringParam(name string) ResolvableString {
	return ResolvableString{
		param: &name,
	}
}

/** Int **/

type ResolvableInt struct {
	value *int32
	param *string
}

func (ri ResolvableInt) Proto() *jobconfigpb.Int32Var {
	if ri.value != nil {
		return &jobconfigpb.Int32Var{
			Kind: &jobconfigpb.Int32Var_Value{
				Value: *ri.value,
			},
		}
	} else if ri.param != nil {
		return &jobconfigpb.Int32Var{
			Kind: &jobconfigpb.Int32Var_Param{
				Param: *ri.param,
			},
		}
	}
	return nil
}

func IntValue(val int) ResolvableInt {
	i32 := int32(val)
	return ResolvableInt{
		value: &i32,
	}
}

func IntParam(name string) ResolvableInt {
	return ResolvableInt{
		param: &name,
	}
}
