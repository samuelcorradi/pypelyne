from pypelyne import Transform as Super

class Transform(Super):

    def process(self):
        """
        """
        if self._dataset.is_empty():
            return
        ds = self._dataset.copy(copy_data=False)
        for _, row in enumerate(self._dataset):
            if ''.join(row).strip():
                ds._data.append(row)
        self._dataset = ds
