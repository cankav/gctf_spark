def get_tensor_index(gtp_spec, tensor_name):
    for tensor_index, tensor in enumerate(gtp_spec['tensors']):
        if tensor['name'] == tensor_name:
            return tensor_index

    raise Exception('tensor_name %s not found in gtp_spec %s' %(tensor_name, gtp_spec))
