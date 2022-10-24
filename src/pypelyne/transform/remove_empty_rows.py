from pypelyne import Transform as Super

class Transform(Super):

    def process(self):
        """
        """
        if self._dataset.is_empty():
            return
        line = self._dataset.first() # le uma linha
        while line:
            if not ''.join(line).strip():
                self._dataset.current_delete()
                try:
                    line = self._dataset.current()
                except:
                    break
                continue
            try:
                line = self._dataset.next()
            except:
                break
